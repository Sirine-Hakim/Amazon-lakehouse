from pathlib import Path

from huggingface_hub import hf_hub_download

N_REVIEWS = 50_000
OUT = Path("data/reviews_sample.jsonl")
OUT.parent.mkdir(exist_ok=True)

path = hf_hub_download(
    repo_id="McAuley-Lab/Amazon-Reviews-2023",
    filename="raw/review_categories/All_Beauty.jsonl",
    repo_type="dataset",
)

count = 0
with open(path, "r", encoding="utf-8") as src, open(OUT, "w", encoding="utf-8") as dst:
    for line in src:
        dst.write(line)
        count += 1
        if count >= N_REVIEWS:
            break

print(f"{count} avis écrits dans {OUT}")