import csv
import math
import random
import os
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd


APPLICANTS_FILE = "20260101_Applicants_v1.csv"
REVIEWERS_FILE = "20260101_reviewers_v1.csv"
ALLOCATION_OUTPUT = "20260101_random_allocation_min2.csv"
ALLOCATION_COUNTS_OUTPUT = "20260101_random_allocation_min2_counts.csv"


MIN_ASSIGNMENTS_PER_REVIEWER = 2

ASSIGNMENTS_PER_APPLICATION = 2

REQUIRE_PREFERRED = True

ALLOW_FALLBACK_NON_PREFERRED = False

SEED = 7

if SEED is not None:
    random.seed(SEED)



def load_reviewer_ids(reviewers_path: Path) -> list[int]:
    ids = []
    with reviewers_path.open(encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s.isdigit():
                ids.append(int(s))
    return ids

def get_preferred_pools(applicants_df: pd.DataFrame) -> tuple[list[list[int]], set[int]]:
    
    preferred_cols_all = [
        "Reviewer 1", "Reviewer 2", "Reviewer 3", "Reviewer 4", "Reviewer 5"
    ]
    preferred_cols = [c for c in preferred_cols_all if c in applicants_df.columns]

    pools = []
    pref_set = set()
    for _, row in applicants_df.iterrows():
        pool = []
        seen = set()
        for col in preferred_cols:
            val = row.get(col)
            if pd.notna(val):
                sval = str(val).strip()
                if sval.isdigit():
                    rid = int(sval)
                    if rid not in seen:
                        pool.append(rid)
                        seen.add(rid)
                        pref_set.add(rid)
        pools.append(pool)
    return pools, pref_set

def initial_allocation(app_ids: list, pools: list[list[int]], reviewer_ids: set[int], k: int) -> tuple[pd.DataFrame, Counter]:
   
    load = Counter()
    rows = []
    for app_id, pool in zip(app_ids, pools):
        picks = []
        if len(pool) >= k:
            picks = random.sample(pool, k)
        else:
            picks = pool.copy()
            
        for rid in picks:
            load[rid] += 1
        row = {"Application ID": app_id}
        for i in range(k):
            row[f"Assigned Reviewer {i+1}"] = picks[i] if i < len(picks) else ""
        row["Preferred Pool Size"] = len(pool)
        row["Used Fallback"] = False
        rows.append(row)
    return pd.DataFrame(rows), load

def top_up_to_min(load: Counter, allocation_df: pd.DataFrame, pools: list[list[int]], app_ids: list[int], min_required: int, reviewer_ids: list[int]) -> None:

    assign_cols = [c for c in allocation_df.columns if c.startswith("Assigned Reviewer ")]

    def is_assigned(i, rid):
        for col in assign_cols:
            if allocation_df.at[i, col] == rid:
                return True
        return False

    def add_to_app(i, rid) -> bool:
        
        for col in assign_cols:
            if allocation_df.at[i, col] in ("", None):
                allocation_df.at[i, col] = rid
                load[rid] += 1
                return True
        
        current = [(col, allocation_df.at[i, col]) for col in assign_cols]
        
        for col, cur_rid in sorted(current, key=lambda kv: load.get(kv[1], 0), reverse=True):
            if not isinstance(cur_rid, int):
                continue
            if load[cur_rid] > min_required:  
                load[cur_rid] -= 1
                allocation_df.at[i, col] = rid
                load[rid] += 1
                return True
        return False

    changed = True
    while changed:
        changed = False
        for rid in reviewer_ids:
            appears_in_any_pool = any(rid in pool for pool in pools)
            if REQUIRE_PREFERRED and not appears_in_any_pool:
                continue
            while load.get(rid, 0) < min_required:
                
                candidate_indices = [i for i, pool in enumerate(pools) if rid in pool and not is_assigned(i, rid)]
                
                def assigned_count(i): return sum(1 for col in assign_cols if allocation_df.at[i, col] not in ("", None))
                candidate_indices.sort(key=lambda i: assigned_count(i))

                placed = False
                for i in candidate_indices:
                    if add_to_app(i, rid):
                        changed = True
                        placed = True
                        break

                if not placed:
                    if ALLOW_FALLBACK_NON_PREFERRED:
                        for i in range(len(app_ids)):
                            if not is_assigned(i, rid):
                                if add_to_app(i, rid):
                                    changed = True
                                    placed = True
                                    break
                    if not placed:
                        
                        break

        
        if all(load.get(r, 0) >= min_required or (REQUIRE_PREFERRED and not any(r in pool for pool in pools)) for r in reviewer_ids):
            break


applicants_path = Path(APPLICANTS_FILE)
reviewers_path = Path(REVIEWERS_FILE)

applicants_df = pd.read_csv(applicants_path)
reviewer_ids_full = load_reviewer_ids(reviewers_path)

app_ids = applicants_df["Application ID"].tolist()
pools, preferred_set = get_preferred_pools(applicants_df)


reviewers_for_min = reviewer_ids_full if not REQUIRE_PREFERRED else sorted(list(preferred_set))

num_apps = len(app_ids)
num_reviewers_min = len(reviewers_for_min)


k = ASSIGNMENTS_PER_APPLICATION
required_total = MIN_ASSIGNMENTS_PER_REVIEWER * num_reviewers_min
available_total = num_apps * k
if available_total < required_total:
    k = math.ceil(required_total / num_apps)
    print(f"Increasing reviewers-per-application to {k} to satisfy minimum {MIN_ASSIGNMENTS_PER_REVIEWER} for {num_reviewers_min} reviewers.")

allocation_df, load_counter = initial_allocation(app_ids, pools, set(reviewer_ids_full), k)
top_up_to_min(load_counter, allocation_df, pools, app_ids, MIN_ASSIGNMENTS_PER_REVIEWER, reviewers_for_min)


allocation_df.to_csv(ALLOCATION_OUTPUT, index=False)

counts_items = sorted(load_counter.items(), key=lambda kv: (-kv[1], kv[0]))
with open(ALLOCATION_COUNTS_OUTPUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["Reviewer ID", "Assigned Count"])
    for rid, cnt in counts_items:
        w.writerow([rid, cnt])

print("Allocation enforcing minimum complete. Output files:")
print(f" - {Path(ALLOCATION_OUTPUT).resolve()}")
print(f" - {Path(ALLOCATION_COUNTS_OUTPUT).resolve()}")


below_min = [rid for rid in reviewers_for_min if len([x for x in counts_items if x[0] == rid]) == 0 or dict(counts_items).get(rid, 0) < MIN_ASSIGNMENTS_PER_REVIEWER]
print(f"Reviewers at/above minimum: {sum(1 for rid in reviewers_for_min if dict(counts_items).get(rid, 0) >= MIN_ASSIGNMENTS_PER_REVIEWER)} / {len(reviewers_for_min)}")
if below_min:
    print("WARNING: Could not reach minimum for these reviewers:")
    print(below_min)