import argparse
import os
from dataclasses import dataclass
from typing import Dict

import pandas as pd


def _norm_col(name: str) -> str:
    return str(name).strip().upper().replace(" ", "_").replace("-", "_").replace("/", "_")


def _parse_prod_lvl(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "":
        return None

    # Hierarchy encoding first: .1, ..2, ...3
    if s.startswith("."):
        tail = s.lstrip(".")
        if tail.isdigit():
            return int(tail)

    # Plain numeric fallback
    if s.isdigit():
        return int(s)
    if s.replace(".", "", 1).isdigit() and s.count(".") <= 1:
        return int(float(s))
    return None


def _norm_find_token(x: str) -> str:
    s = str(x).strip()
    if s == "":
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return s


def _find_set_per_edge(df_edges: pd.DataFrame, col_name: str) -> pd.DataFrame:
    tmp = df_edges[["edge_key", "find_no"]].copy()
    tmp["find_no"] = tmp["find_no"].fillna("").astype(str).str.strip()
    agg = (
        tmp.groupby("edge_key", as_index=False)["find_no"]
        .agg(lambda s: "|".join(sorted(set([_norm_find_token(x) for x in s.tolist() if x != ""]))))
        .rename(columns={"find_no": col_name})
    )
    return agg


def build_dev_edges(dev_path: str, root_item: str) -> pd.DataFrame:
    dev = pd.read_csv(dev_path)
    for c in ["ROOT_ITEM_ID", "PARENT_ITEM_ID", "CHILD_ITEM_ID"]:
        dev[c] = dev[c].astype(str).str.strip()

    dev = dev[dev["ROOT_ITEM_ID"] == root_item].copy()
    dev["BILL_OF_MATERIAL_LEVEL_NBR"] = pd.to_numeric(dev["BILL_OF_MATERIAL_LEVEL_NBR"], errors="coerce")
    dev = dev.dropna(subset=["PARENT_ITEM_ID", "CHILD_ITEM_ID", "BILL_OF_MATERIAL_LEVEL_NBR"]).copy()
    dev = dev[(dev["PARENT_ITEM_ID"] != "") & (dev["CHILD_ITEM_ID"] != "")]

    dev["child_level"] = dev["BILL_OF_MATERIAL_LEVEL_NBR"].astype(int)
    dev["parent_level"] = dev["child_level"] - 1

    out = dev[["PARENT_ITEM_ID", "CHILD_ITEM_ID", "parent_level", "child_level"]].copy()
    out.columns = ["parent_item", "child_item", "parent_level", "child_level"]

    if "BILL_OF_MATERIAL_FIND_NBR" in dev.columns:
        out["find_no"] = dev["BILL_OF_MATERIAL_FIND_NBR"].astype(str).str.strip()
    else:
        out["find_no"] = ""

    out["source"] = "DEV"
    return out


def build_prod_edges(prod_path: str, sheet_name: str) -> pd.DataFrame:
    prod = pd.read_excel(prod_path, sheet_name=sheet_name)
    prod.columns = [_norm_col(c) for c in prod.columns]

    if "ITEM_NO" not in prod.columns or "LVL" not in prod.columns:
        raise KeyError(f"ITEM_NO/LVL missing in PROD sample. Columns: {list(prod.columns)}")

    find_col = "FIND_NO" if "FIND_NO" in prod.columns else ("FINDNO" if "FINDNO" in prod.columns else None)
    work = prod[["ITEM_NO", "LVL"]].copy()
    work["find_no"] = prod[find_col].astype(str).str.strip() if find_col else ""
    work["item_no"] = work["ITEM_NO"].astype(str).str.strip()
    work["level"] = work["LVL"].apply(_parse_prod_lvl)

    work = work.dropna(subset=["item_no", "level"]).copy()
    work = work[work["item_no"] != ""].copy()
    work["level"] = work["level"].astype(int)

    last_item_at_level = {}
    edges = []

    for _, row in work.iterrows():
        lvl = int(row["level"])
        item = row["item_no"]
        find_no = str(row["find_no"]).strip()

        if lvl > 0 and (lvl - 1) in last_item_at_level:
            edges.append(
                {
                    "parent_item": last_item_at_level[lvl - 1],
                    "child_item": item,
                    "parent_level": lvl - 1,
                    "child_level": lvl,
                    "find_no": find_no,
                    "source": "PROD",
                }
            )

        last_item_at_level[lvl] = item
        for k in [k for k in list(last_item_at_level.keys()) if k > lvl]:
            del last_item_at_level[k]

    if not edges:
        raise ValueError("No PROD edges inferred from ITEM_NO/LVL order")

    return pd.DataFrame(edges)


def add_edge_keys(df_edges: pd.DataFrame) -> pd.DataFrame:
    out = df_edges.copy()
    out["parent_item"] = out["parent_item"].astype(str).str.strip().str.upper()
    out["child_item"] = out["child_item"].astype(str).str.strip().str.upper()
    out["parent_level"] = out["parent_level"].astype(int)
    out["child_level"] = out["child_level"].astype(int)
    out["edge_key"] = (
        out["parent_item"]
        + "|L"
        + out["parent_level"].astype(str)
        + "->"
        + out["child_item"]
        + "|L"
        + out["child_level"].astype(str)
    )
    return out


def level_continuity_issues(df_edges: pd.DataFrame) -> pd.DataFrame:
    chk = df_edges.copy()
    chk["level_delta"] = chk["child_level"] - chk["parent_level"]
    return chk[chk["level_delta"] != 1].copy()


def orphan_parent_issues(df_edges: pd.DataFrame) -> pd.DataFrame:
    # Orphan definition:
    # A non-root parent node (not L0) that appears as a parent in DEV edges
    # but never appears as a child in any DEV edge.
    pnodes = set(df_edges["parent_item"] + "|L" + df_edges["parent_level"].astype(str))
    cnodes = set(df_edges["child_item"] + "|L" + df_edges["child_level"].astype(str))
    orphan_nodes = sorted([n for n in pnodes if (n not in cnodes) and not n.endswith("|L0")])
    return pd.DataFrame(
        {
            "orphan_parent_node": orphan_nodes,
            "orphan_definition": "Non-root parent node appears as parent but never as child in DEV",
            "flag_logic": "parent_node_not_in_child_nodes and level != 0",
        }
    )


@dataclass
class CheckResult:
    summary: pd.DataFrame
    outputs: Dict[str, pd.DataFrame]


def run_checks(root_item: str, dev_csv_path: str, prod_xlsx_path: str, prod_sheet: str) -> CheckResult:
    dev_edges = add_edge_keys(build_dev_edges(dev_csv_path, root_item))
    prod_edges = add_edge_keys(build_prod_edges(prod_xlsx_path, prod_sheet))

    keys_dev = dev_edges[["edge_key", "parent_item", "parent_level", "child_item", "child_level"]].drop_duplicates()
    keys_prod = prod_edges[["edge_key", "parent_item", "parent_level", "child_item", "child_level"]].drop_duplicates()

    # PROD is ground truth.
    missing_in_dev = keys_prod.merge(keys_dev[["edge_key"]], on="edge_key", how="left", indicator=True)
    missing_in_dev = missing_in_dev[missing_in_dev["_merge"] == "left_only"].drop(columns=["_merge"])

    # Drift signal: in DEV but not in PROD.
    extra_in_dev = keys_dev.merge(keys_prod[["edge_key"]], on="edge_key", how="left", indicator=True)
    extra_in_dev = extra_in_dev[extra_in_dev["_merge"] == "left_only"].drop(columns=["_merge"])

    matched_edges = keys_prod.merge(keys_dev[["edge_key"]], on="edge_key", how="inner")

    find_dev_set = _find_set_per_edge(dev_edges, "find_no_dev_set")
    find_prod_set = _find_set_per_edge(prod_edges, "find_no_prod_set")
    find_cmp = (
        matched_edges[["edge_key"]]
        .drop_duplicates()
        .merge(find_dev_set, on="edge_key", how="left")
        .merge(find_prod_set, on="edge_key", how="left")
    )
    find_cmp["find_no_dev_set"] = find_cmp["find_no_dev_set"].fillna("")
    find_cmp["find_no_prod_set"] = find_cmp["find_no_prod_set"].fillna("")
    find_mismatch = find_cmp[find_cmp["find_no_dev_set"] != find_cmp["find_no_prod_set"]].copy()

    level_issues_dev = level_continuity_issues(dev_edges)
    orphan_dev = orphan_parent_issues(dev_edges)

    coverage_pct = (len(matched_edges) / len(keys_prod) * 100.0) if len(keys_prod) else 0.0

    summary = pd.DataFrame(
        [
            {"metric": "prod_expected_edge_count", "value": len(keys_prod)},
            {"metric": "dev_observed_edge_count", "value": len(keys_dev)},
            {"metric": "matched_edge_count", "value": len(matched_edges)},
            {"metric": "dev_coverage_pct_of_prod", "value": round(coverage_pct, 2)},
            {"metric": "missing_in_dev_count", "value": len(missing_in_dev)},
            {"metric": "extra_in_dev_count", "value": len(extra_in_dev)},
            {"metric": "find_mismatch_count", "value": len(find_mismatch)},
            {"metric": "level_issues_dev_count", "value": len(level_issues_dev)},
            {"metric": "orphan_dev_count", "value": len(orphan_dev)},
        ]
    )

    outputs = {
        "summary": summary,
        "missing_in_dev": missing_in_dev,
        "extra_in_dev": extra_in_dev,
        "find_mismatch": find_mismatch,
        "level_issues_dev": level_issues_dev,
        "orphan_dev": orphan_dev,
    }

    return CheckResult(summary=summary, outputs=outputs)


def save_outputs(outputs: Dict[str, pd.DataFrame], report_dir: str, run_tag: str) -> None:
    os.makedirs(report_dir, exist_ok=True)
    for name, table in outputs.items():
        path = os.path.join(report_dir, f"{run_tag}_{name}.csv")
        table.to_csv(path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run DEV vs PROD BOM data-quality checks (PROD as source of truth)."
    )
    parser.add_argument("--root-item", required=True)
    parser.add_argument("--dev-csv-path", required=True)
    parser.add_argument("--prod-xlsx-path", required=True)
    parser.add_argument("--prod-sheet", required=True)
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--run-tag", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_tag = args.run_tag if args.run_tag else args.root_item

    result = run_checks(
        root_item=args.root_item,
        dev_csv_path=args.dev_csv_path,
        prod_xlsx_path=args.prod_xlsx_path,
        prod_sheet=args.prod_sheet,
    )
    save_outputs(result.outputs, args.report_dir, run_tag)

    print("Saved report files:")
    for name in result.outputs.keys():
        print(f"- {os.path.join(args.report_dir, f'{run_tag}_{name}.csv')}")

    print("\nOrphan definition:")
    print(
        "A DEV orphan is a non-root node (level > 0) that appears as a parent in at least one edge, "
        "but never appears as a child in any DEV edge."
    )
    print("Flag rule: parent_node_not_in_child_nodes and level != 0")

    print("\nSummary:")
    print(result.summary.to_string(index=False))


if __name__ == "__main__":
    main()
