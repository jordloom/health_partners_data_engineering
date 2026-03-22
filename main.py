import os
import re
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

THEME = 'Hospitals'
API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DATA_DIR = "hospital_datasets"
META_FILE = os.path.join(DATA_DIR, "metadata.json")
MAX_WORKERS = 5

def to_snake_case(s):
    s = s.lower()
    s = re.sub(r"[’'\".,\-()/]", '', s)
    s = re.sub(r"[^a-z0-9]+", '_', s)
    s = re.sub(r"__+", '_', s)
    return s.strip('_')

def load_metadata():
    if os.path.exists(META_FILE):
        with open(META_FILE, "r") as f:
            return json.load(f)
    return {}

def save_metadata(meta):
    with open(META_FILE, "w") as f:
        json.dump(meta, f, indent=2)

def process_dataset(ds, meta):
    dist = ds.get('distribution', [])
    if not dist:
        return None
    url = dist[0].get('downloadURL')
    if not url or not url.endswith('.csv'):
        return None
    dataset_id = ds['identifier']
    modified = ds.get('modified')
    fname = f"{dataset_id}.csv"
    out_path = os.path.join(DATA_DIR, fname)
    # Check if already up-to-date
    if meta.get(dataset_id, '') == modified and os.path.exists(out_path):
        return None
    # Download CSV
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to download {url}")
        return None
    # Process CSV
    try:
        df = pd.read_csv(pd.compat.StringIO(resp.text))
    except Exception:
        # fallback to file-based read for large files
        tmp_path = out_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(resp.content)
        df = pd.read_csv(tmp_path)
        os.remove(tmp_path)
    # Convert columns to snake_case
    df.columns = [to_snake_case(col) for col in df.columns]
    df.to_csv(out_path, index=False)
    print(f"Downloaded and processed: {fname}")
    return dataset_id, modified

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    meta = load_metadata()
    resp = requests.get(API_URL)
    resp.raise_for_status()
    data = resp.json()
    hospital_datasets = [ds for ds in data if ds.get('theme') == [THEME]]
    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for ds in hospital_datasets:
            tasks.append(executor.submit(process_dataset, ds, meta))
        for task in tasks:
            result = task.result()
            if result:
                dataset_id, modified = result
                meta[dataset_id] = modified
    save_metadata(meta)

if __name__ == "__main__":
    main()