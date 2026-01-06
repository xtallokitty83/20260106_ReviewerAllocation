
import csv
import math
import random
import re
import os
from pathlib import Path
from typing import List, Set, Dict, Optional

import pandas as pd

os.chdir ("C://Users/ls13g17/OneDrive - University of Southampton/Documents/CaSDaR/Funding call")

APPLICANTS_FILE = "20260101_Applicants_v2.csv"
REVIEWERS_FILE  = "20260101_reviewers_v1.csv"
OUTPUT_FILE     = "20260101_random_allocation_no_conflicts.csv"

ASSIGNMENTS_PER_APPLICATION = 3   # reviewers per application
SEED = 42                         # set to None for non-deterministic randomness

# Columns in Applicants that list reviewers to EXCLUDE for that application
EXCLUSION_COL_CANDIDATES = [
    "Reviewer 1", "Reviewer 2", "Reviewer 3"]

# -------- Robust ID normalizer --------
ID_PATTERN_INT_OR_FLOAT = re.compile(r"^\s*(\d+)(?:\.0+)?\s*$", re.IGNORECASE)

def normalize_id(val) -> Optional[int]:
    """
    Convert a cell value into an integer ID if it is effectively an integer.
    Handles:
      - int / float (e.g., 138590 or 138590.0)
      - digit strings ("138590")
      - digit-with-decimal-zero strings ("138590.0")
      - scientific notation ("1.3859E+05") -> 138590 if integral
      - strings with commas ("138,590")
    Returns None if it can't be safely interpreted as an integer ID.
    """
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None

    # Already an int
    if isinstance(val, int):
        return int(val)

    # Float that is integral
    if isinstance(val, float):
        if val.is_integer():
            return int(val)
        return None  # non-integer floats are not valid IDs

    # String-like
    s = str(val).strip()
    if not s:
        return None

    # Remove thousands separators
    s_no_commas = s.replace(",", "")

    # Try plain int or 'int with .0'
    m = ID_PATTERN_INT_OR_FLOAT.match(s_no_commas)
    if m:
        return int(m.group(1))

    # Try scientific notation -> float -> int if integral
    try:
        f = float(s_no_commas)
        if f.is_integer():
            return int(f)
    except ValueError:
        pass

    return None

# -------- Utilities --------
def load_reviewer_ids(reviewers_path: Path) -> List[int]:
    """
    Load reviewer IDs from reviewers CSV (column 1). Robust to header and numeric formats.
    """
    ids: List[int] = []
    with reviewers_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            raw = row[0]
            rid = normalize_id(raw)
            # Skip header or non-numeric
            if rid is None:
                continue
            ids.append(rid)
    # Deduplicate and keep stable order
    seen = set()
    uniq = []
    for rid in ids:
        if rid not in seen:
            uniq.append(rid)
            seen.add(rid)
    return uniq

def get_exclusion_cols(df: pd.DataFrame) -> List[str]:
    """Return the exclusion column names that actually exist in the Applicants CSV."""
    # Be tolerant of extra spaces/case by exact match first; you can extend to case-insensitive if needed.
    return [c for c in EXCLUSION_COL_CANDIDATES if c in df.columns]

def build_exclusion_set(row: pd.Series, exclusion_cols: List[str]) -> Set[int]:
    """Build a set of normalized reviewer IDs listed for this application (to be excluded)."""
    excluded: Set[int] = set()
    for col in exclusion_cols:
        val = row.get(col)
        rid = normalize_id(val)
        if rid is not None:
            excluded.add(rid)
    return excluded

def choose_reviewers(eligible_pool: List[int], k: int) -> List[int]:
    """Randomly choose up to k distinct reviewers from the eligible pool."""
    if len(eligible_pool) <= k:
        return random.sample(eligible_pool, len(eligible_pool)) if eligible_pool else []
    return random.sample(eligible_pool, k)

# -------- Main allocation --------
def main():
    if SEED is not None:
        random.seed(SEED)

    applicants_path = Path(APPLICANTS_FILE)
    reviewers_path  = Path(REVIEWERS_FILE)

    # Load data
    df = pd.read_csv(applicants_path)
    if "Application ID" not in df.columns:
        raise ValueError("Applicants CSV must contain an 'Application ID' column.")

    exclusion_cols = get_exclusion_cols(df)
    if not exclusion_cols:
        print("WARNING: No exclusion columns found; only global random allocation will be performed.")

    reviewer_ids = load_reviewer_ids(reviewers_path)
    if not reviewer_ids:
        raise ValueError("No reviewer IDs found in reviewers CSV.")

    global_reviewers = reviewer_ids[:]  # already normalized ints
    # Optionally shuffle for randomness
    random.shuffle(global_reviewers)

    output_rows: List[Dict[str, object]] = []
    warnings: List[str] = []

    for _, row in df.iterrows():
        app_id = row.get("Application ID")

        excluded = build_exclusion_set(row, exclusion_cols)
        # Eligible = all reviewers minus excluded (types are consistent ints)
        eligible = [rid for rid in global_reviewers if rid not in excluded]

        picks = choose_reviewers(eligible, ASSIGNMENTS_PER_APPLICATION)

        if len(picks) < ASSIGNMENTS_PER_APPLICATION:
            warnings.append(
                f"Application {app_id}: only {len(picks)} eligible reviewers "
                f"(needed {ASSIGNMENTS_PER_APPLICATION})."
            )

        # Final safety check: no overlap
        if set(picks) & excluded:
            raise RuntimeError(
                f"Conflict detected for Application {app_id}: assigned excluded reviewers {set(picks) & excluded}"
            )

        out = {"Application ID": app_id}
        for i in range(ASSIGNMENTS_PER_APPLICATION):
            out[f"Assigned Reviewer {i+1}"] = picks[i] if i < len(picks) else ""
        out["Excluded Count"] = len(excluded)
        out["Eligible Pool Size"] = len(eligible)
        out["Used Fallback"] = len(picks) < ASSIGNMENTS_PER_APPLICATION
        output_rows.append(out)

    pd.DataFrame(output_rows).to_csv(OUTPUT_FILE, index=False)

    print(f"Random allocation complete (excluded reviewers never assigned).")
    print(f"Output file: {Path(OUTPUT_FILE).resolve()}")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(" -", w)

if __name__ == "__main__":
    main()