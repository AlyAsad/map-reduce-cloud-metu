from collections import Counter, defaultdict
from celery import Celery
import orjson
import json

# redis setup
REDIS_URL = "redis://localhost:6379/0"
app = Celery('hw3', broker=REDIS_URL, backend=REDIS_URL)

# input data setup
DATA_FILE = "900k Definitive Spotify Dataset.json"
CHUNK = 10000



# reading chunks of size 10000 and returning them all
def read_chunks(path=DATA_FILE, chunk=CHUNK):
    block = []
    with open(path, 'rb') as f:
        for line in f:
            track = orjson.loads(line)
            block.append(track)
            if len(block) >= chunk:
                yield block
                block = []
    if block:
        yield block




# MAIN MAP FUNCTION
@app.task
def map_chunk(chunk):
    totals = 0.0
    counts = 0
    artist_popularity = defaultdict(lambda: [0, 0])
    explicit_pop = defaultdict(lambda: [0, 0])
    artist_counts = Counter()
    dance_year = defaultdict(float)
    dance_year_count = defaultdict(int)

    for track in chunk:
        
        # duration in seconds
        length_str = track.get("Length", "0:00")
        m, s = length_str.split(":")
        dur = int(m) * 60 + int(s)

        # year extracted from Release Date
        year_val = 0
        rd = track.get("Release Date", "0000-00-00")
        year_val = int(rd[:4]) if rd else 0

        # popularity
        pop_val = int(track.get("Popularity", 0))

        # explicit flag
        explicit = track.get("Explicit", "No").lower()

        # artist name
        artists = track.get("Artist(s)", "")

        # danceability
        dance_val = float(track.get("Danceability", 0))




        # accumulating
        totals += dur
        counts += 1
        explicit_pop[explicit][0] += pop_val
        explicit_pop[explicit][1] += 1
        dance_year[year_val] += dance_val
        dance_year_count[year_val] += 1
        
        if "," not in artists:
            artist = artists.strip()
            artist_popularity[artist][0] += pop_val
            artist_popularity[artist][1] += 1
            artist_counts[artist] += 1


    return {
        "tot_sec": totals,
        "count": counts,
        "artist_popularity": dict(artist_popularity),
        "explicit_pop": dict(explicit_pop),
        "artist_counts": dict(artist_counts),
        "dance_year": dict(dance_year),
        "dance_year_count": dict(dance_year_count)
    }




# MAIN REDUCE FUNCTION
@app.task
def reduce_all(results):
    agg_tot = 0.0
    agg_count = 0
    agg_artist_popularity = defaultdict(lambda: [0, 0])
    agg_explicit = defaultdict(lambda: [0, 0])
    agg_art_counts = Counter()
    agg_dance = defaultdict(float)
    agg_dance_count = defaultdict(int)
    
    
    # accumulating the results
    for res in results:
        agg_tot += res["tot_sec"]
        agg_count += res["count"]

        for art, (pop_sum, cnt) in res.get("artist_popularity", {}).items():
            agg_artist_popularity[art][0] += pop_sum
            agg_artist_popularity[art][1] += cnt

        for k, (s, c) in res.get("explicit_pop", {}).items():
            agg_explicit[k][0] += s
            agg_explicit[k][1] += c


        agg_art_counts.update(res["artist_counts"])

        for y, d in res.get("dance_year", {}).items():
            agg_dance[y] += d

        for y, c in res.get("dance_year_count", {}).items():
            agg_dance_count[y] += c


    
    
    
    
    # top 100 artists and their average popularity
    top100 = agg_art_counts.most_common(100)
    artist_popularity = {
        art: round(agg_artist_popularity[art][0] / agg_artist_popularity[art][1], 2)
        if agg_artist_popularity[art][1] > 0 else 0
        for art, _ in top100
    }
    
    
    
    
    # average explicit popularity
    explicit_avg = {
    'yes': round(agg_explicit['yes'][0] / agg_explicit['yes'][1], 2),
    'no': round(agg_explicit['no'][0] / agg_explicit['no'][1], 2),
}


    

    # danceability by buckets
    def bucket(y):
        y = int(y)
        if y <= 2001:
            return 'before-2001'
        elif 2001 < y <= 2012:
            return '2001-2012'
        else:
            return 'after-2012'

    bucket_sum = defaultdict(float)
    bucket_cnt = defaultdict(int)
    
    

    for y in agg_dance:
        b = bucket(y)
        bucket_sum[b] += agg_dance[y]
        bucket_cnt[b] += agg_dance_count[y]
    
    ordered_buckets = ['before-2001', '2001-2012', 'after-2012']
    
    dance_by_year = {
        b: round(bucket_sum[b] / bucket_cnt[b], 2) if bucket_cnt[b] else 0
        for b in ordered_buckets
    }





    # final structure
    final = {
        "total": int(agg_tot),
        "average": round(agg_tot / agg_count, 2) if agg_count else 0,
        "artist-popularity": artist_popularity,
        "explicit-popularity": explicit_avg,
        "dancebyyear": dance_by_year,
    }

    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    return "results.json created"
