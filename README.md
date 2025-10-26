# Sports Prediction Tally API

FastAPI service + CLI that:
- Searches Google (via Serper.dev or SerpAPI) for queries like “Texans vs 49ers prediction”
- Reads the first N results
- Strictly filters to articles published in the past N days
- Extracts the predicted **winner**
- Returns a tally per team + source list

## Contents

- `app.py` – FastAPI wrapper exposing `POST /tally`
- `tally_predictions.py` – CLI that performs the search/extraction and writes a CSV (and optional Markdown)
- `requirements.txt` – Python dependencies
- `render.yaml` – Render deployment (web service + optional scheduled job)
- `.env.example` – Template for API keys
- `.gitignore` – Standard ignores + outputs

---

## 1) Local Setup

```bash
python -m venv venv
source venv/bin/activate             # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                 # add SERPER_API_KEY or SERPAPI_KEY
