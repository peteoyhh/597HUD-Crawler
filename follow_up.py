#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import time
import pandas as pd
from datetime import datetime, UTC
from googleapiclient.discovery import build

API_KEY = os.getenv("API_KEY") 
youtube = build("youtube", "v3", developerKey=API_KEY)

DATA_DIR = "datas/"
FOLLOWUP_DIR = "followups/"
os.makedirs(FOLLOWUP_DIR, exist_ok=True)

def load_all_video_ids(data_dir):
    video_ids = set()
    for fname in os.listdir(data_dir):
        if fname.endswith(".csv") and fname.startswith("us_"):
            fpath = os.path.join(data_dir, fname)
            try:
                df = pd.read_csv(fpath, usecols=["video_id"])
                df["video_id"] = df["video_id"].astype(str).str.strip()
                video_ids.update(df["video_id"].dropna().tolist())
                print(f"✅ read {fname}: {len(df)} ")
            except Exception as e:
                print(f"⚠️ can't read {fname}: {e}")
    return sorted(list(video_ids))

def fetch_video_stats(video_ids):
    all_data = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            req = youtube.videos().list(part="statistics", id=",".join(batch))
            resp = req.execute()
            for item in resp.get("items", []):
                stats = item.get("statistics", {}) or {}
                video = {
                    "video_id": item["id"],
                    "views": int(stats.get("viewCount", 0) or 0),
                    "likes": int(stats.get("likeCount", 0) or 0),
                    "comments": int(stats.get("commentCount", 0) or 0),
                    "crawl_date": datetime.now(UTC).isoformat(),
                }
                all_data.append(video)
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ rows {i//50+1} error: {e}")
            time.sleep(2)
    return pd.DataFrame(all_data)

if __name__ == "__main__":

    video_ids = load_all_video_ids(DATA_DIR)

    df_update = fetch_video_stats(video_ids)

    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = os.path.join(FOLLOWUP_DIR, f"followup_{date_str}.csv")
    df_update.to_csv(out_path, index=False, encoding="utf-8-sig")



# In[ ]:




