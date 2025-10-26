import json, os, re, subprocess, tempfile, csv
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any

app = FastAPI(title="Sports Prediction Tally API")

# ---------- Schemas ----------

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
    provider: Literal["serper","serpapi"] = "serper"

class SourceRow(BaseModel):
    published_utc: Optional[str]
    domain: str
    url: str
    page_title: str
    result_title: str
    winner: Literal["A","B","ambiguous"]
    winner_method: str
    match_phrase: str
    bet_type: Literal["moneyline","spread","unknown"]

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

# Batch
class BatchItem(BaseModel):
    query: str
    team_a: List[str]
    team_b: List[str]
    results: int = 50
    days: int = 5
    allow: Optional[List[str]] = None
    provider: Literal["serper","serpapi"] = "serper"

class BatchRequest(BaseModel):
    items: List[BatchItem]

class BatchGameSummary(BaseModel):
    game: str
    team_a_label: str
    team_b_label: str
    votes_team_a: int
    votes_team_b: int
    ambiguous: int
    dominant: Optional[str]  # "Team A" | "Team B" | "Tie"
    provider: str
    days: int

class BatchResponse(BaseModel):
    summaries: List[BatchGameSummary]
    table: List[Dict[str, Any]]  # flat rows for easy DataFrame use
    detail: Dict[str, TallyResponse]  # keyed by game id

# ---------- Helpers ----------

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
                winner=(r.get("winner","ambiguous") or "ambiguous"),
                winner_method=r.get("winner_method",""),
                match_phrase=r.get("match_phrase",""),
                bet_type=(r.get("bet_type","unknown") or "unknown"),
            ))
    return out

# ---------- Endpoints ----------

@app.get("/")
def root():
    return {
        "service": "sports-tally-scraper",
        "status": "ok",
        "endpoints": {
            "docs": "/docs",
            "openapi": "/openapi.json",
            "tally": "POST /tally",
            "tally_batch": "POST /tally/batch"
        }
    }

@app.get("/tally")
def tally_info():
    return {
        "message": "Use POST /tally with JSON body. See /docs for schema.",
        "example_request": {
            "query":"Texans vs 49ers prediction",
            "team_a": ["Houston Texans","Texans","Houston"],
            "team_b": ["San Francisco 49ers","49ers","San Francisco","SF"],
            "results": 50,
            "days": 5
        }
    }

@app.post("/tally", response_model=TallyResponse)
def tally(req: TallyRequest):
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
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Runner error: {e}")

    if p.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Scraper failed:\nSTDERR:\n{p.stderr}\nSTDOUT:\n{p.stdout}")

    rows = csv_to_rows(out_csv)

    votes_a = sum(1 for r in rows if r.winner == "A")
    votes_b = sum(1 for r in rows if r.winner == "B")
    ambig   = sum(1 for r in rows if r.winner == "ambiguous")

    return TallyResponse(
        query=req.query,
        team_a_label=req.team_a[0],
        team_b_label=req.team_b[0],
        days=req.days,
        results_requested=req.results,
        votes_team_a=votes_a,
        votes_team_b=votes_b,
        ambiguous=ambig,
        sources=rows
    )

@app.post("/tally/batch", response_model=BatchResponse)
def tally_batch(body: BatchRequest):
    summaries: List[BatchGameSummary] = []
    table_rows: List[Dict[str, Any]] = []
    detail: Dict[str, TallyResponse] = {}

    for item in body.items:
        out_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        allow = ",".join(item.allow) if item.allow else ""
        team_a = ",".join(item.team_a)
        team_b = ",".join(item.team_b)

        cmd = [
            "python", "tally_predictions.py",
            "--provider", item.provider,
            "--query", item.query,
            "--team-a", team_a,
            "--team-b", team_b,
            "--results", str(item.results),
            "--days", str(item.days),
            "--out", out_csv
        ]
        if allow:
            cmd += ["--allow", allow]

        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Runner error (batch item): {e}")

        if p.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Scraper failed for '{item.query}':\n{p.stderr}\n{p.stdout}")

        rows = csv_to_rows(out_csv)
        votes_a = sum(1 for r in rows if r.winner == "A")
        votes_b = sum(1 for r in rows if r.winner == "B")
        ambig   = sum(1 for r in rows if r.winner == "ambiguous")

        dominant = "Tie"
        if votes_a > votes_b: dominant = "Team A"
        elif votes_b > votes_a: dominant = "Team B"

        game_id = f"{item.team_a[0]} vs {item.team_b[0]}"

        # Per-game summary
        summaries.append(BatchGameSummary(
            game=game_id,
            team_a_label=item.team_a[0],
            team_b_label=item.team_b[0],
            votes_team_a=votes_a,
            votes_team_b=votes_b,
            ambiguous=ambig,
            dominant=dominant,
            provider=item.provider,
            days=item.days
        ))

        # Detail payload (re-using TallyResponse)
        detail[game_id] = TallyResponse(
            query=item.query,
            team_a_label=item.team_a[0],
            team_b_label=item.team_b[0],
            days=item.days,
            results_requested=item.results,
            votes_team_a=votes_a,
            votes_team_b=votes_b,
            ambiguous=ambig,
            sources=rows
        )

        # Flat table rows (ready for DataFrame)
        for r in rows:
            table_rows.append({
                "game": game_id,
                "team_a": item.team_a[0],
                "team_b": item.team_b[0],
                "winner": r.winner,
                "winner_method": r.winner_method,
                "bet_type": r.bet_type,
                "published_utc": r.published_utc,
                "domain": r.domain,
                "url": r.url,
                "page_title": r.page_title,
                "result_title": r.result_title,
                "match_phrase": r.match_phrase
            })

    return BatchResponse(summaries=summaries, table=table_rows, detail=detail)
