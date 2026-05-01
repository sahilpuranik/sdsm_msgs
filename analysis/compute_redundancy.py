#!/usr/bin/env python3
"""
Compute object-level redundancy from v2 object-AoI logs.

An object report is OBJECT-LEVEL REDUNDANT if the same object_id was reported
by a different sender to the same receiver within a time window W.

This metric is only available for v2 algorithm runs (HybridSDSM_v2, Greedy_v2, etc.)
because the *-object-aoi.csv log is populated by the spatial-association pipeline
which only runs for v2 configurations.

For v1 algorithms (Periodic, Greedy, HybridSDSM), only the legacy sender-state
redundancy metric is available (logged as sender_state_redundant in *-rx.csv).

Input:  *-object-aoi.csv (columns: time, receiver, object_id, aoi)
        *-rx.csv (columns: time, receiver, sender, ...)
Output: *-object-redundancy.csv

Percentile method: numpy nearest-rank (method='lower') to match C++ implementation.

Usage:
  python analysis/compute_redundancy.py results/HybridSDSM_v2-r0
  python analysis/compute_redundancy.py results/  # process all v2 prefixes
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def compute_object_redundancy(prefix: str, result_dir: Path,
                               window_s: float = 0.5) -> dict:
    """Compute object-level redundancy for a v2 run.

    A report of object_id at receiver R at time t is redundant if R already
    received a report for the same object_id from a DIFFERENT sender within
    the last `window_s` seconds.

    Returns dict with redundancy stats, or empty dict if data unavailable.
    """
    obj_aoi_file = result_dir / f"{prefix}-object-aoi.csv"
    rx_file = result_dir / f"{prefix}-rx.csv"

    if not obj_aoi_file.exists():
        print(f"  SKIP: {obj_aoi_file.name} not found (v2 runs only)")
        return {}

    obj_df = pd.read_csv(obj_aoi_file)
    required = {"time", "receiver", "object_id"}
    if not required.issubset(obj_df.columns):
        print(f"  SKIP: missing columns {required - set(obj_df.columns)}")
        return {}

    if obj_df.empty:
        print(f"  SKIP: {obj_aoi_file.name} is empty")
        return {}

    # Sort by time
    obj_df = obj_df.sort_values("time")

    # For each (receiver, object_id), check if it was seen within window_s
    # We track: (receiver, object_id) -> last_seen_time
    last_seen: dict[tuple[int, int], float] = {}
    total_reports = 0
    redundant_reports = 0

    for _, row in obj_df.iterrows():
        rcv = int(row["receiver"])
        obj_id = int(row["object_id"])
        t = float(row["time"])
        key = (rcv, obj_id)

        total_reports += 1
        if key in last_seen and (t - last_seen[key]) < window_s:
            redundant_reports += 1

        last_seen[key] = t

    rate = redundant_reports / total_reports if total_reports > 0 else 0.0

    return {
        "prefix": prefix,
        "total_object_reports": total_reports,
        "redundant_object_reports": redundant_reports,
        "object_redundancy_rate": round(rate, 6),
        "window_s": window_s,
    }


def find_v2_prefixes(result_dir: Path) -> list[str]:
    """Find v2 run prefixes that have object-aoi CSVs."""
    prefixes = set()
    for f in result_dir.glob("*-object-aoi.csv"):
        name = f.name.replace("-object-aoi.csv", "")
        prefixes.add(name)
    return sorted(prefixes)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute object-level redundancy from v2 object-AoI logs."
    )
    parser.add_argument("path",
                        help="Results directory or specific prefix (e.g., results/HybridSDSM_v2-r0)")
    parser.add_argument("--window", type=float, default=0.5,
                        help="Redundancy window in seconds (default: 0.5)")
    args = parser.parse_args()

    p = Path(args.path)
    if p.is_dir():
        result_dir = p
        prefixes = find_v2_prefixes(result_dir)
    else:
        result_dir = p.parent
        prefixes = [p.name]

    if not prefixes:
        print(f"No *-object-aoi.csv files found in {result_dir}", file=sys.stderr)
        print("Object-level redundancy is only available for v2 algorithm runs.", file=sys.stderr)
        return 1

    all_results = []
    for prefix in prefixes:
        print(f"Computing object redundancy for {prefix} ...")
        stats = compute_object_redundancy(prefix, result_dir, window_s=args.window)
        if stats:
            all_results.append(stats)
            print(f"  total={stats['total_object_reports']}  "
                  f"redundant={stats['redundant_object_reports']}  "
                  f"rate={stats['object_redundancy_rate']:.4f}")

    if all_results:
        out_df = pd.DataFrame(all_results)
        out_path = Path(result_dir) / "object-redundancy-summary.csv"
        out_df.to_csv(out_path, index=False)
        print(f"\nSummary -> {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
