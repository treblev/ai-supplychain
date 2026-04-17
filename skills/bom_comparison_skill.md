---
name: bom-data-quality-check-skill
description: Run BOM hierarchy data quality checks between two sets of BOM data, export report CSVs, and explain missing edges, drift edges, level issues, find-number mismatches, and orphans in plain English.
---

# BOM Data Quality Check Skill

Use this skill when you need to validate DEV BOM hierarchy quality against a PROD sample for a specific root item.

Treat PROD as source of truth and run the deterministic script `bom_data_quality_skill.py` so output is consistent across all models.

Run command pattern:

```powershell
python bom_data_quality_skill.py \
  --root-item <ROOT_ITEM> \
  --dev-csv-path "<DEV_CSV_PATH>" \
  --prod-xlsx-path "<PROD_XLSX_PATH>" \
  --prod-sheet <PROD_SHEET> \
  --report-dir "<REPORT_DIR>" \
  --run-tag <RUN_TAG>
```

Checks performed:
- `missing_in_dev`: edge exists in PROD but not in DEV.
- `extra_in_dev`: edge exists in DEV but not in PROD.
- `find_mismatch`: edge exists in both but normalized find-number sets differ.
- `level_issues_dev`: `child_level - parent_level != 1`.
- `orphan_dev`: non-root parent node appears as parent but never as child in DEV.

Orphan definition in plain English:
- A DEV orphan is a non-root node (level > 0) that appears as a parent in at least one DEV edge, but never appears as a child in any DEV edge.

Orphan flagging logic in plain English:
- Build all parent nodes as `ITEM|Lx`.
- Build all child nodes as `ITEM|Lx`.
- Flag parent nodes not present in child-node set, excluding `L0` roots.

Required output behavior:
- Export CSVs: `summary`, `missing_in_dev`, `extra_in_dev`, `find_mismatch`, `level_issues_dev`, `orphan_dev`.
- Return a concise summary with key metrics and top findings.
- Include top 10 rows each for `missing_in_dev`, `extra_in_dev`, and `orphan_dev` unless user requests a different count.

## Examples
- Example usage 1: "Run BOM checks for root `99CG11` and summarize coverage + top 10 missing DEV edges."
- Example usage 2: "Re-run with a new PROD sheet and compare whether orphan count improved from previous run."
- Example usage 3: "Run data quality check on BOM data in Dev1 environment for item X against the SPEED BOM in production."

## Guidelines
- Always treat PROD as truth unless the user explicitly overrides this rule.
- Keep explanations in plain English and include the orphan definition whenever orphan results are shown.
- Do not alter source files; write results to report CSVs in the configured output folder.
- If required columns are missing, stop with a clear error listing available columns.
