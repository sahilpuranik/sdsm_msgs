# veins_ros_v2v_ucla

V2V **Sensor Data Sharing (SDSM)** dissemination benchmark on a **UCLA-area SUMO network**, using **Veins** (OMNeT++ + SUMO) with optional ROS 2 bridge. The stack uses **full IEEE 802.11p PHY/MAC** (path loss, fading, interference, CSMA/CA) instead of toy drop/delay models.

**Canonical paper comparison (v2):** `Periodic`, `Greedy_v2`, and `HybridSDSM_v2` at **~400 vehicles**, **300 s**, **seed 0** (effective `num_vehicles` in `*-metadata.csv` may be slightly below the requested `-n` due to SUMO insertion dynamics). See `CLAUDE.md` for the latest headline numbers and `REWRITE_SUMMARY.md` for design provenance.

---

## Simulation stack

| Component | Role |
|-----------|------|
| **[SUMO](https://eclipse.dev/sumo/)** | Traffic: car-following, lanes, signals on the UCLA-area net. |
| **[OMNeT++](https://omnetpp.org/)** | Discrete-event core, modules, `.ini` / `.ned`. |
| **[Veins](https://veins.car2x.org/)** | TraCI bridge, `Mac1609_4`, `Decider80211p`, mobility. |
| **ROS 2** (optional) | UDP bridge for live TX/RX; not needed for batch CSV runs. |

Each **TraCI step (0.1 s)** SUMO advances vehicles; Veins syncs OMNeT++ `Car` modules. **`RosSDSMApp`** decides when to send and which objects to pack, then the MAC/PHY delivers or drops packets. Successful receptions are logged to CSV under `results/` (see `.gitignore`: raw `simulations/results/` is local scratch).

---

## Simulation world (current defaults)

| Aspect | Setting |
|--------|---------|
| Playground | **1300 m × 1000 m** (`simulations/omnetpp.ini`) |
| Channel | **`config.xml`:** `SimplePathlossModel` **α = 2.75**, **Nakagami** **m = 1.5** (constM); **5.89 GHz** decider |
| TX power | **100 mW (20 dBm)** |
| Noise / sensitivity | **−98 dBm** noise floor, **−110 dBm** min power (typical Veins 802.11p) |
| Interference range | **`maxInterfDist = 1500 m`** (avoids clipping far interferers) |
| Obstacle shadowing | **Off** (no building polygons); metadata records `obstacle_shadowing,false` |
| Fleet size | Set via `run_experiments.py --num-vehicles N`; **metadata `num_vehicles`** is the **effective** count for that run |

---

## Algorithms

### Canonical v2 (primary comparison)

All three share the **same PHY/MAC, SDSM schema, and (for the two adaptive policies) the same 100 ms evaluation tick**.

| Algorithm | When to send | What goes in the SDSM (objects) |
|-----------|--------------|----------------------------------|
| **`Periodic`** | Fixed **10 Hz** (`sendInterval = 0.1 s`) | **Distance top‑K** (closest neighbors first), **K ≤ 32** |
| **`Greedy_v2`** | **`evaluateV2Schedule`:** parallel thresholds on self-change, object-set change, time; **OR** combine; **CBR suppressor**; **backstop** at **T_max**; **min inter-send** (backstop can override) | **Distance top‑K**, **K ≤ 32** |
| **`HybridSDSM_v2`** | **Same scheduler code** as `Greedy_v2` (reason prefix `hybrid_*` vs `greedy_*` in `*-triggers.csv`) | **RX spatial association** → **LARM-style redundancy window** → **Lyu-style VoI** → **top‑K**; **confidence** is **`conf = 1.0`** until a perception module supplies scores |

**Factor isolation**

- **Periodic vs Greedy_v2:** isolates **scheduler** (fixed vs adaptive).
- **Greedy_v2 vs HybridSDSM_v2:** isolates **object selection** (same scheduler).

Scheduler logic is **ETSI TS 103 324 / TS 102 687–inspired**, not a conformance certification. See comments in `src/RosSDSMApp.cc` (`evaluateV2Schedule`).

### Legacy v1 (preserved, not canonical for the current study)

`Greedy`, `EventTriggered`, `GreedyBSMImplied`, `HybridSDSM` (`hybridVariant="v1"`) remain in `simulations/omnetpp.ini` for reproducibility. They use the older weighted-sum / v1 hybrid paths. Prefer v2 configs for new results.

---

## SDSM payload (J3224-aligned)

- Up to **`K_max = 32`** objects; **`SDSM_PER_OBJECT_BYTES = 26`** in code; **`obj_measurement_time_ms`** per object for freshness/AoI-style use.
- Objects are drawn from **`neighborInfo_`** within **`detectionRange`** (default **300 m**) and **`detectionMaxAge`** (default **2 s**).

---

## Metrics and CSVs

### Summary (`*-summary.csv`)

| Column | Meaning |
|--------|---------|
| `total_tx`, `total_rx` | Global SDSM send / successful receive counts |
| `avg_one_way_latency`, `p95_*`, `p99_*` | **`simTime − sendTimestamp`** at receiver (seconds). **Not** Kaul et al. sawtooth AoI. |
| `sender_state_redundancy_rate` | Fraction of RX where sender’s position+speed delta vs **previous** message **< epsilon** |
| `avg_throughput` | **`(total_rx / sim_duration) / num_vehicles`** — **receptions per second per vehicle** (not bytes/s) |
| `pdr_legacy_all_pairs` | **`total_rx / (total_tx × (num_vehicles−1))`** broadcast-style PDR (optimistic denominator); **distance-binned** PDR: `analysis/compute_pdr.py` |
| `avg_object_aoi`, `p95_*`, `p99_*` | v2 sampler: age of last update per tracked object; **−1** if unused (e.g. Periodic). Absolute seconds can be **inflated by stale tracks** — compare **across algorithms** or condition on distance/relevance offline. |
| `assoc_*` | Spatial-association stage totals (**HybridSDSM_v2**; **0** for Greedy_v2 / Periodic) |

### Per-vehicle (`*-vehicle-summary.csv`)

Includes `avg_latency` / `p95_latency` from **`simTime − BSM envelope timestamp`**, and `avg_one_way_latency` / `p95_one_way_latency` from **payload `sendTimestamp`** (different clocks).

### Reception log (`*-rx.csv`)

`time,receiver,sender,message_id,one_way_latency,inter_arrival,snr,rss_dbm,distance_to_sender,packet_size,cbr,num_objects,delta_state,sender_state_redundant`

### v2-only logs

- **`*-triggers.csv`:** one row per vehicle per tick (`greedy_*` / `hybrid_*` reasons).
- **`*-object-aoi.csv`:** 100 ms samples `(receiver, object_id, aoi)`.

### Post-processing (repo)

| Script | Purpose |
|--------|---------|
| `analysis/compute_aoi.py` | Kaul-style AoI from full `*-rx.csv` |
| `analysis/compute_pdr.py` | Distance-binned PDR |
| `analysis/compute_redundancy.py` | Object-level redundancy (v2 + object-AoI) |
| `analysis/build_claude_batch_30mb.py` | Downsampled export ≤30 MB/file + `MANIFEST.txt` |

Column semantics for external LLM/batch work: `analysis/CLAUDE_PROMPT_presentation_batches.md`.

---

## Limitations (disclose in papers/talks)

1. **No urban obstacle shadowing** — LOS-style links with fading only.
2. **Hybrid confidence** not driven by a real detector (**`conf = 1.0`**).
3. **Association** uses **greedy** one-to-one matching after Mahalanobis gating, not an optimal assignment solver.
4. **ETSI-inspired** scheduler/suppressor — not a standards compliance claim.
5. **`num_vehicles`** may be **< requested N**; always use metadata for fair normalization.
6. **Single-scenario / seed** until you publish multi-seed CIs.

---

## Sources (design + standards)

- **SAE J3224** — SDSM structure.
- **SAE J2945/1** — periodic safety messaging context (10 Hz baseline).
- **ETSI TS 103 324** — parallel-threshold style motivation (scheduler).
- **ETSI TS 102 687** — DCC / CBR motivation.
- **S. Kaul, R. Yates, M. Gruteser** — Age of Information (for `compute_aoi.py` definition).
- **T. Thandavarayan et al., JNCA 2023** — LARM (redundancy gate inspiration).
- **X. Lyu et al., IEEE VNC 2025** — VoI-style object ranking.
- **C. Sommer et al.** — adaptive beaconing / self-change literature (v1 Greedy lineage).

ROS message alignment: [ucla-mobility/CPX-SDSM](https://github.com/ucla-mobility/CPX-SDSM).

---

## Setup

**Prerequisites:** OMNeT++ 6.0+, Veins 5.2+, SUMO 1.8+. Set `*.manager.commandLine` in `simulations/omnetpp.ini` to your `sumo` binary.

```bash
source <omnetpp-install>/setenv
cd src && make -j$(nproc)
```

---

## Running

### Canonical v2 (example)

```bash
cd /path/to/veins_ros_v2v_ucla
python3 run_experiments.py -a Periodic      -s 0 -n 400 --skip-probe
python3 run_experiments.py -a Greedy_v2     -s 0 -n 400 --skip-probe
python3 run_experiments.py -a HybridSDSM_v2 -s 0 -n 400 --skip-probe
```

Artifacts: **`results/n400/<Algorithm>/seed0/`** (and optional symlinks under `results/n400_seed0_bundle/` if you use them).

### Legacy v1 examples

```bash
python3 run_experiments.py --algorithm Greedy --sim-duration 90
python3 run_experiments.py --algorithm HybridSDSM --sim-duration 90
```

### Shrink huge RX CSV (optional)

```bash
python3 scripts/shrink_rx_csv.py results/n400/Periodic/seed0/Periodic-r0-rx.csv --every 4 --decimals 3
```

---

## ROS 2 bridge (optional)

```bash
cd ros2_ws && colcon build && source install/setup.bash
ros2 run veins_ros_bridge udp_bridge_node --ros-args -p udp_port:=50010
```

Enable with `rosBridgeMode = "live"` in `omnetpp.ini` when needed.

---

## License

See upstream [ucla-mobility/CPX-SDSM](https://github.com/ucla-mobility/CPX-SDSM).
