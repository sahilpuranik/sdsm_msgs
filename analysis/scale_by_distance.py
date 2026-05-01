#!/usr/bin/env python3
"""Aggregate scale-with-distance metrics across density x algorithm runs.

For every run under results/n<N>/<algo>/seed<k>/:
  - Reads *-rx.csv (per-decode log with distance_to_sender).
  - Reads *-tx.csv (per-TX events) and *-timeseries.csv (positions at ~1 Hz)
    to reconstruct the per-distance-bin "potential receiver" denominator.
  - Emits one row per (run, distance bin) with:
        n_rx, n_tx_denominator, pdr,
        mean_snr, mean_rss, mean_aoi, redundancy_rate, mean_num_objects.

The goal is a tiny aggregate CSV (<< 30 MB) summarising scale-with-distance
behaviour so it can be handed off to an LLM for plotting/writing.

PDR denominator:
  For each TX at time t by sender i, look up positions of every other
  vehicle alive at time bucket floor(t) and tally them into distance bins
  around the sender. Sum over all TXs. Numerator is the count of rx-rows
  per distance bin.
"""

from __future__ import annotations

import argparse
import re
import sys
import time as _time
from pathlib import Path

import numpy as np
import pandas as pd


BIN_EDGES = np.arange(0.0, 525.0, 25.0)
BIN_LO = BIN_EDGES[:-1]
BIN_HI = BIN_EDGES[1:]
BIN_CENTERS = 0.5 * (BIN_LO + BIN_HI)
N_BINS = len(BIN_LO)

DENSITY_DIRS = ["n50", "n100", "n225", "n400", "n600"]
ALGOS = ["Periodic", "Greedy", "HybridSDSM"]


def bin_distances(d: np.ndarray) -> np.ndarray:
    """Return bin index per distance; -1 if outside [0, BIN_EDGES[-1])."""
    idx = np.digitize(d, BIN_EDGES, right=False) - 1
    idx = np.where((idx >= 0) & (idx < N_BINS), idx, -1)
    return idx


def aggregate_rx(rx_path: Path) -> dict:
    cols = ["distance_to_sender", "snr", "rss_dbm", "aoi",
            "num_objects", "redundant"]
    dtype = {
        "distance_to_sender": "float32",
        "snr": "float32",
        "rss_dbm": "float32",
        "aoi": "float32",
        "num_objects": "int32",
        "redundant": "int8",
    }
    df = pd.read_csv(rx_path, usecols=cols, dtype=dtype)

    d = df["distance_to_sender"].to_numpy()
    bins = bin_distances(d)
    mask = bins >= 0
    bins = bins[mask]

    snr = df["snr"].to_numpy()[mask].astype(np.float64)
    rss = df["rss_dbm"].to_numpy()[mask].astype(np.float64)
    aoi = df["aoi"].to_numpy()[mask].astype(np.float64)
    nobj = df["num_objects"].to_numpy()[mask].astype(np.float64)
    red = df["redundant"].to_numpy()[mask].astype(np.float64)

    # SNR is logged in linear units; its distribution has a heavy tail at
    # short range. Averaging in dB is what's physically meaningful.
    snr_db = 10.0 * np.log10(np.clip(snr, 1e-30, None))

    n_rx = np.bincount(bins, minlength=N_BINS).astype(np.int64)
    sum_snr_db = np.bincount(bins, weights=snr_db, minlength=N_BINS)
    sum_rss = np.bincount(bins, weights=rss, minlength=N_BINS)
    sum_aoi = np.bincount(bins, weights=aoi, minlength=N_BINS)
    sum_nobj = np.bincount(bins, weights=nobj, minlength=N_BINS)
    n_red = np.bincount(bins, weights=red, minlength=N_BINS).astype(np.int64)

    return dict(n_rx=n_rx, sum_snr_db=sum_snr_db, sum_rss=sum_rss, sum_aoi=sum_aoi,
                sum_nobj=sum_nobj, n_red=n_red)


def compute_denominator(tx_path: Path, ts_path: Path) -> np.ndarray:
    """Per-bin count of (TX, potential-receiver-in-bin) pairs."""
    tx = pd.read_csv(tx_path, usecols=["time", "node"],
                     dtype={"time": "float64", "node": "int32"})
    ts = pd.read_csv(ts_path,
                     usecols=["time", "vehicle_id", "position_x", "position_y"],
                     dtype={"time": "float64", "vehicle_id": "int32",
                            "position_x": "float32", "position_y": "float32"})

    # Bucket timeseries and TX by integer second. Each vehicle samples ~1 Hz
    # (staggered), so floor(time) gives a dense snapshot per second.
    ts["t_bucket"] = np.floor(ts["time"]).astype(np.int64)
    tx["t_bucket"] = np.floor(tx["time"]).astype(np.int64)

    # One position per (vehicle, bucket): take the last sample in the bucket.
    ts_snap = (ts.sort_values(["vehicle_id", "time"])
                 .drop_duplicates(["vehicle_id", "t_bucket"], keep="last"))

    # Compress TX into (bucket, sender, count) tuples.
    tx_groups = (tx.groupby(["t_bucket", "node"])
                   .size().reset_index(name="n_tx"))

    # Build per-bucket arrays: vehicle_id, x, y.
    bucket_to_arrays: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for b, grp in ts_snap.groupby("t_bucket", sort=False):
        bucket_to_arrays[int(b)] = (
            grp["vehicle_id"].to_numpy(),
            grp["position_x"].to_numpy(),
            grp["position_y"].to_numpy(),
        )

    denom = np.zeros(N_BINS, dtype=np.int64)
    missing_sender = 0
    missing_bucket = 0

    # For robustness, if a sender has no position in its TX bucket, fall back
    # to the nearest bucket within +/-1.
    neighbor_offsets = (0, -1, 1)

    for _, row in tx_groups.iterrows():
        b = int(row["t_bucket"])
        sender = int(row["node"])
        n_tx = int(row["n_tx"])

        vids = xs = ys = None
        for off in neighbor_offsets:
            arr = bucket_to_arrays.get(b + off)
            if arr is None:
                continue
            mask = arr[0] == sender
            if mask.any():
                vids, xs, ys = arr
                sx = float(xs[mask][0])
                sy = float(ys[mask][0])
                break
        else:
            missing_sender += 1
            continue

        # Use the bucket where the sender was found for the receiver set.
        keep = vids != sender
        if not keep.any():
            continue
        dx = xs[keep] - sx
        dy = ys[keep] - sy
        d = np.sqrt(dx * dx + dy * dy)
        bins = bin_distances(d)
        bins = bins[bins >= 0]
        if bins.size == 0:
            continue
        hist = np.bincount(bins, minlength=N_BINS).astype(np.int64)
        denom += hist * n_tx

    if missing_sender:
        print(f"    [warn] sender position unavailable for {missing_sender} TX groups")
    if missing_bucket:
        print(f"    [warn] bucket empty for {missing_bucket} TX groups")
    return denom


def parse_n(dir_name: str) -> int:
    m = re.match(r"n(\d+)$", dir_name)
    if not m:
        raise ValueError(dir_name)
    return int(m.group(1))


def process_run(results_root: Path, n: int, algo: str, seed: int):
    run_dir = results_root / f"n{n}" / algo / f"seed{seed}"
    rx_path = run_dir / f"{algo}-r{seed}-rx.csv"
    tx_path = run_dir / f"{algo}-r{seed}-tx.csv"
    ts_path = run_dir / f"{algo}-r{seed}-timeseries.csv"
    if not (rx_path.exists() and tx_path.exists() and ts_path.exists()):
        print(f"[skip] missing files in {run_dir}")
        return None

    size_mb = rx_path.stat().st_size / 1e6
    print(f"[run] n={n} algo={algo} seed={seed}  rx={size_mb:.0f} MB")
    t0 = _time.time()

    rx = aggregate_rx(rx_path)
    t1 = _time.time()
    print(f"    rx aggregated in {t1 - t0:.1f}s  (total rx rows in bins: {int(rx['n_rx'].sum()):,})")

    denom = compute_denominator(tx_path, ts_path)
    t2 = _time.time()
    print(f"    denominator in   {t2 - t1:.1f}s  (total denom pairs: {int(denom.sum()):,})")

    safe_div = lambda num, den: np.where(den > 0, num / np.maximum(den, 1), np.nan)
    out = pd.DataFrame({
        "distance_bin_lo": BIN_LO,
        "distance_bin_hi": BIN_HI,
        "distance_bin_center": BIN_CENTERS,
        "n": n,
        "algo": algo,
        "seed": seed,
        "n_rx": rx["n_rx"],
        "n_tx_denominator": denom,
        "pdr": safe_div(rx["n_rx"], denom),
        "mean_snr_db": safe_div(rx["sum_snr_db"], rx["n_rx"]),
        "mean_rss_dbm": safe_div(rx["sum_rss"], rx["n_rx"]),
        "mean_aoi_s": safe_div(rx["sum_aoi"], rx["n_rx"]),
        "redundancy_rate": safe_div(rx["n_red"], rx["n_rx"]),
        "mean_num_objects": safe_div(rx["sum_nobj"], rx["n_rx"]),
    })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--out", type=Path,
                    default=Path("analysis/corrections/distance_sweep.csv"))
    ap.add_argument("--densities", type=str, nargs="+", default=DENSITY_DIRS)
    ap.add_argument("--algos", type=str, nargs="+", default=ALGOS)
    ap.add_argument("--seeds", type=int, nargs="+", default=[0])
    args = ap.parse_args()

    frames = []
    for dens_dir in args.densities:
        n = parse_n(dens_dir)
        for algo in args.algos:
            for seed in args.seeds:
                df = process_run(args.results, n, algo, seed)
                if df is not None:
                    frames.append(df)

    if not frames:
        print("No runs processed.")
        return 1

    combined = pd.concat(frames, ignore_index=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(args.out, index=False, float_format="%.6g")
    print(f"\nWrote {args.out}  rows={len(combined)}  "
          f"size={args.out.stat().st_size / 1024:.1f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
