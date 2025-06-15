# tests/test_quality_score.py
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import polars_bio as pb


BASE_DIR = Path(__file__).resolve().parent.parent
FASTQ = BASE_DIR / "tests" / "data" / "quality_score" / "example.fastq"
REPORT_HTML = BASE_DIR / "tests" / "data" / "quality_score" / "report.html"

assert FASTQ.exists(), f"Brak pliku {FASTQ}"
assert REPORT_HTML.exists(), f"Brak pliku {REPORT_HTML}"


class TestSequenceQualityScore:
    _ = pb.read_fastq(str(FASTQ))

    # Histogram z udaf sequence_quality_score() w SQL
    _hist_sql = pd.Series(
        json.loads(
            pb.sql(
                """
                SELECT sequence_quality_score(quality_scores) AS global_hist
                FROM   example
                """
            )
            .collect()
            .to_pandas()
            .loc[0, "global_hist"]
        ),
        dtype=int,
    ).rename_axis("score").sort_index()
    _hist_sql.index = _hist_sql.index.astype(int)      # klucze → int

    # Histogram z report.html
    _hist_html = None
    _html_text = REPORT_HTML.read_text(encoding="utf-8", errors="ignore")
    _m = re.search(
        r"var\s+qualSpec\s*=\s*({.*?\"Sequence quality scores\".*?});",
        _html_text,
        flags=re.S,
    )
    if _m:
        _values = json.loads(_m.group(1))["data"]["values"]
        _hist_html = pd.Series(
            {int(v["score"]): int(v["count"]) for v in _values},
            dtype=int,
        ).rename_axis("score").sort_index()
    else:
        raise AssertionError("Nie znaleziono bloku 'qualSpec' w report.html")

    def test_histogram_equal(self):
        pd.testing.assert_series_equal(
            self._hist_sql,
            self._hist_html,
            check_dtype=False,
        )
