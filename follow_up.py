#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import time
import pandas as pd
from datetime import datetime, UTC
from googleapiclient.discovery import build

# ====== 基础配置 ======
API_KEY = os.getenv("API_KEY")  # ✅ 从 GitHub Secrets 读取
youtube = build("youtube", "v3", developerKey=API_KEY)

DATA_DIR = "datas/"
FOLLOWUP_DIR = "followups/"
os.makedirs(FOLLOWUP_DIR, exist_ok=True)

# ====== 读取所有 video_id ======
def load_all_video_ids(data_dir):
    video_ids = set()
    for fname in os.listdir(data_dir):
        if fname.endswith(".csv") and fname.startswith("us_"):
            fpath = os.path.join(data_dir, fname)
            try:
                df = pd.read_csv(fpath, usecols=["video_id"])
                df["video_id"] = df["video_id"].astype(str).str.strip()
                video_ids.update(df["video_id"].dropna().tolist())
                print(f"✅ 读取 {fname}: {len(df)} 条记录")
            except Exception as e:
                print(f"⚠️ 无法读取 {fname}: {e}")
    return sorted(list(video_ids))

# ====== 批量抓取最新统计数据 ======
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
            print(f"❌ 批次 {i//50+1} 出错: {e}")
            time.sleep(2)
    return pd.DataFrame(all_data)

# ====== 主流程 ======
if __name__ == "__main__":
    print("🚀 开始追踪 YouTube 视频最新数据...\n")

    video_ids = load_all_video_ids(DATA_DIR)
    print(f"\n📊 共收集 {len(video_ids)} 个视频 ID\n")

    df_update = fetch_video_stats(video_ids)
    print(f"✅ 成功抓取 {len(df_update)} 条记录")

    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = os.path.join(FOLLOWUP_DIR, f"followup_{date_str}.csv")
    df_update.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"📦 已保存至: {out_path}")
    print("🎯 今日任务完成！")


# In[ ]:




