#!/usr/bin/env python3
"""
Density sweep: run 3 algorithms × 5 vehicle densities × 1 seed = 15 simulations.

Calls run_experiments.py for each (algorithm, density) pair. The first run
includes the setup probe; subsequent runs skip it for speed.

Usage:
  python run_density_sweep.py                  # all 15 runs, 90s each
  python run_density_sweep.py --sim-duration 60
  python run_density_sweep.py --dry-run
"""

import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DENSITIES = [50, 100, 225, 400, 600]
ALGORITHMS = [
    "Periodic", "Greedy", "HybridSDSM",
    "HybridSDSM_v2", "Greedy_v2", "EventTriggered_v2",
]
SEED = 0
SIM_DURATION = 90

def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Density sweep for V2X SDSM benchmark.")
    parser.add_argument("--sim-duration", type=int, default=SIM_DURATION,
                        help=f"Sim duration in seconds (default: {SIM_DURATION})")
    parser.add_argument("--dry-run", action="store_true", help="Print plan and exit")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    runner = root / "run_experiments.py"

    runs = [(n, algo) for n in DENSITIES for algo in ALGORITHMS]
    total = len(runs)

    if args.dry_run:
        print(f"Density sweep: {total} runs (densities={DENSITIES}, algorithms={ALGORITHMS}, seed={SEED})")
        for i, (n, algo) in enumerate(runs, 1):
            print(f"  [{i:2d}/{total}] n={n:>3d}  {algo}")
        return 0

    print(f"=== Density Sweep: {total} runs, {args.sim_duration}s each ===")
    print(f"Densities: {DENSITIES}")
    print(f"Algorithms: {ALGORITHMS}")
    print()

    start_all = datetime.now(timezone.utc)
    probe_done = False
    failures = []

    for i, (n, algo) in enumerate(runs, 1):
        print(f"[{i:2d}/{total}] n={n}, {algo} ...", flush=True)
        run_start = time.time()

        cmd = [
            sys.executable, str(runner),
            "--algorithm", algo,
            "--seed", str(SEED),
            "--sim-duration", str(args.sim_duration),
            "--num-vehicles", str(n),
        ]
        if probe_done:
            cmd.append("--skip-probe")

        result = subprocess.run(cmd, cwd=str(root))

        elapsed = time.time() - run_start
        if result.returncode != 0:
            print(f"  FAIL (exit {result.returncode}, {elapsed:.0f}s)")
            failures.append((n, algo))
        else:
            print(f"  OK ({elapsed:.0f}s)")
            probe_done = True

        remaining = total - i
        if remaining > 0:
            print(f"  {remaining} runs left")

    total_elapsed = (datetime.now(timezone.utc) - start_all).total_seconds()
    print(f"\n=== Done. {total - len(failures)}/{total} succeeded in {total_elapsed/60:.1f} min ===")
    if failures:
        print("Failures:")
        for n, algo in failures:
            print(f"  n={n}, {algo}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
