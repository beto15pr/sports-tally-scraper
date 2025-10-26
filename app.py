import json, os, re, subprocess, tempfile, csv
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional

app = FastAPI(title="Sports Prediction Tally API")

class TallyRequest(BaseModel):
    query: str = Field(..., example="Texans vs 49ers prediction")
    team_a: List[str] = Field(..., example=["Houston Texans","Texans","Houston"])
    team_b: List[str] = Field(..., example=["San Francisco 49ers","49ers","San Francisco","SF"])
    results: int = 50
    days: int = 5
    allow: Optional[List[str]] = Field(default=[
        "espn.com","actionnetwork.com","covers.com","pickswise.com","rotowire.com",
        "usatoday.com","sportingnews.com","cbssports.com","oddsshark.com"
    ])
    provider: str = "serper"  # or "serpapi"

class SourceRow(BaseModel):
    published_utc: Optional[str]
    domain: str
    url: str
    page_title: str
    result_title: str
    winner: str
    winner_method: str
    match_phrase: str

class TallyResponse(BaseModel):
    query: str
    team_a_label: str
    team_b_label: str
    days: int
    results_requested: int
    votes_team_a: int
    votes_team_b: int
    ambiguous: int
    sources: List[SourceRow]

def csv_to_rows(path: str) -> List[SourceRow]:
    out = []
    if not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            out.append(SourceRow(
                published_utc=r.get("published_utc") or None,
                domain=r.get("domain",""),
                url=r.get("url",""),
                page_title=r.get("page_title",""),
                result_title=r.get("result_title",""),
                winner=r.get("winner",""),
                winner_method=r.get("winner_method",""),
                match_phrase=r.get("match_phrase",""),
            ))
    return out

@app.post("/tally", response_model=TallyResponse)
def tally(req: TallyRequest):
    # Build CLI args for tally_predictions.py
    out_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    allow = ",".join(req.allow) if req.allow else ""
    team_a = ",".join(req.team_a)
    team_b = ",".join(req.team_b)

    cmd = [
        "python", "tally_predictions.py",
        "--provider", req.provider,
        "--query", req.query,
        "--team-a", team_a,
        "--team-b", team_b,
        "--results", str(req.results),
        "--days", str(req.days),
        "--out", out_csv
    ]
    if allow:
        cmd += ["--allow", allow]

    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runner error: {e}")

    if p.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Scraper failed:\nSTDERR:\n{p.stderr}\nSTDOUT:\n{p.stdout}")

    rows = csv_to_rows(out_csv)

    votes_a = sum(1 for r in rows if r.winner == "A")
    votes_b = sum(1 for r in rows if r.winner == "B")
    ambig   = sum(1 for r in rows if r.winner == "ambiguous")

    team_a_label = req.team_a[0]
    team_b_label = req.team_b[0]

    return TallyResponse(
        query=req.query,
        team_a_label=team_a_label,
        team_b_label=team_b_label,
        days=req.days,
        results_requested=req.results,
        votes_team_a=votes_a,
        votes_team_b=votes_b,
        ambiguous=ambig,
        sources=rows
    )

