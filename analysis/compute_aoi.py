#!/usr/bin/env python3
"""
Compute true Age-of-Information (AoI) from rx CSV logs.

AoI definition (Kaul et al., Infocom 2012):
  AoI(t) at receiver R for sender S = t - timestamp_of_last_successfully_received_update_from_S

This is the sawtooth freshness metric, NOT the per-packet one-way latency
(which is logged as `one_way_latency` in the rx CSV).

Input:  *-rx.csv files (columns: time, receiver, sender, one_way_latency, ...)
Output: *-aoi-derived.csv (time-binned AoI per receiver-sender pair)
        Aggregate stats printed to stdout.

Percentile method: numpy nearest-rank (method='lower') to match C++ implementation.

Usage:
  python analysis/compute_aoi.py results/Periodic-r0-rx.csv
  python analysis/compute_aoi.py results/  # process all *-rx.csv in directory
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def compute_aoi_for_file(rx_path: Path, time_bin_s: float = 1.0) -> pd.DataFrame:
    """Compute per-receiver true AoI sawtooth from an rx CSV.

    For each receiver, we track the freshest update time across all senders.
    AoI at sample time t = t - max_over_senders(last_rx_time[sender]).

    Returns a DataFrame with columns: time_bin, receiver, aoi
    """
    df = pd.read_csv(rx_path)

    # Ensure required columns exist
    required = {"time", "receiver", "sender"}
    if not required.issubset(df.columns):
        print(f"  SKIP {rx_path.name}: missing columns {required - set(df.columns)}")
        return pd.DataFrame()

    df = df.sort_values("time")

    # For each receiver, compute AoI at each time bin
    t_min = df["time"].min()
    t_max = df["time"].max()
    time_bins = np.arange(t_min, t_max + time_bin_s, time_bin_s)

    rows = []
    receivers = df["receiver"].unique()

    for rcv in receivers:
        rcv_df = df[df["receiver"] == rcv].sort_values("time")
        # Track last rx time per sender
        last_rx_time: dict[int, float] = {}

        rx_idx = 0
        for t in time_bins:
            # Process all RX events up to this time bin
            while rx_idx < len(rcv_df) and rcv_df.iloc[rx_idx]["time"] <= t:
                row = rcv_df.iloc[rx_idx]
                sender = int(row["sender"])
                last_rx_time[sender] = row["time"]
                rx_idx += 1

            if last_rx_time:
                # AoI = current time - time of freshest update from any sender
                freshest = max(last_rx_time.values())
                aoi = t - freshest
                rows.append({"time_bin": t, "receiver": rcv, "aoi": aoi})

    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute true AoI from rx CSV logs.")
    parser.add_argument("path", help="Path to *-rx.csv file or directory containing them")
    parser.add_argument("--time-bin", type=float, default=1.0,
                        help="Time bin width in seconds (default: 1.0)")
    args = parser.parse_args()

    p = Path(args.path)
    if p.is_file():
        files = [p]
    elif p.is_dir():
        files = sorted(p.glob("*-rx.csv"))
    else:
        print(f"ERROR: {p} not found", file=sys.stderr)
        return 1

    if not files:
        print(f"No *-rx.csv files found in {p}", file=sys.stderr)
        return 1

    for rx_file in files:
        print(f"Processing {rx_file.name} ...")
        aoi_df = compute_aoi_for_file(rx_file, time_bin_s=args.time_bin)
        if aoi_df.empty:
            continue

        # Write per-receiver AoI
        out_name = rx_file.name.replace("-rx.csv", "-aoi-derived.csv")
        out_path = rx_file.parent / out_name
        aoi_df.to_csv(out_path, index=False, float_format="%.3f")
        print(f"  -> {out_path}")

        # Aggregate stats
        aoi_vals = aoi_df["aoi"].values
        # Percentile method: nearest-rank (lower) to match C++ implementation
        print(f"  AoI stats: mean={np.mean(aoi_vals):.3f}s  "
              f"p95={np.percentile(aoi_vals, 95, method='lower'):.3f}s  "
              f"p99={np.percentile(aoi_vals, 99, method='lower'):.3f}s  "
              f"N={len(aoi_vals)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
""", "Description": "A1b: Post-processing script to compute true receiver-sender AoI sawtooth from rx CSV logs. Uses nearest-rank percentile to match C++ implementation.", "IsArtifact": false, "Overwrite": false, "TargetFile": "/Users/sahilpuranik/Downloads/veins_ros_v2v_ucla/analysis/compute_aoi.py"}
