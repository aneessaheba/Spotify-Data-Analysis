# Spotify Data Analytics

# &#x20;

## Abstract

This report presents **Spotify Data Analysis**, an end‑to‑end data warehousing and business intelligence solution that converts raw Spotify streaming metadata into actionable insights. The system integrates a cloud‑based **ETL pipeline** (Python on AWS Glue) with a **Snowflake** analytical warehouse and **Power BI** dashboards. It enables analysis of listening trends, device usage, repeat play behavior, and the influence of audio features on engagement. Key findings (2023–2024) include: \~**59%** of listening on weekends, **peak activity between 18:00–02:00**, sustained artist loyalty (e.g., The Beatles leading overall plays), and repeat‑play clusters around **10–25 plays** per track with an average listen window of **2–4 minutes**.

---

## 1. Introduction

Digital music platforms generate high‑volume interaction data (plays, skips, device, and temporal context). Stakeholders—artists, labels, and marketers—often lack a consistent pipeline that transforms these events into trustworthy, queryable insights. **Spotify Data Analysis** addresses this gap by delivering a governed warehouse and curated semantic model backed by reproducible transformations and a user‑friendly dashboard.

**Objectives**

* Build a scalable, auditable ETL that ingests Spotify Web API data on a schedule.
* Model a star‑schema warehouse optimized for analytical queries and BI.
* Deliver interactive dashboards for trend analysis, cohorting, and drill‑downs.
* Surface decision‑grade metrics (e.g., top artists, weekend bias, device mix, YoY change).

---

## 2. System Architecture & Data Flow

**Components**

* **Source**: Spotify Web API (tracks, albums, artists, audio features; popularity; user/device interaction signals where available).
* **Compute/Orchestration**: AWS Glue Jobs (Python) & Crawlers; CloudWatch for logging/alerts; retry/backoff.
* **Warehouse**: Snowflake (staging + conformed layers) with partitioning and clustering by **year**, **genre**, and **sentiment score**.
* **BI**: Power BI Desktop report (`.pbix`) connecting to Snowflake via DirectQuery/Import (as configured), with semantic measures in DAX.

**High‑Level Flow**

1. **Extract** from Spotify API → 2) **Transform & Validate** in AWS Glue (Pandas) → 3) **Load** curated tables to Snowflake → 4) **Visualize** in Power BI with slicers, drillthrough, and YoY analysis.

---

## 3. Data Sources

* **Spotify Web API** via Python libraries (`spotipy`, `requests`).
* Entities retrieved include: track metadata (name, artist, album, release date), audio features (tempo, energy, valence, danceability, etc.), and interaction fields (popularity, play count, device type where applicable).
* Ingestion **frequency**: daily/weekly, configurable by job schedule.

---

## 4. ETL Methodology

### 4.1 Extract

* **Authentication**: Client ID/Secret with redirect URIs; tokens stored securely via environment variables on AWS.
* **Pagination & Rate Limits**: Batched requests; exponential backoff and retries; idempotent run guards.
* **Landing**: Raw JSON persisted to S3 (date‑partitioned) for reproducibility and reprocessing.

### 4.2 Transform (AWS Glue + Pandas)

* **Standardization**: Lower‑cased column names; spaces replaced with underscores; consistent datetime/timezone normalization.
* **Cleaning**: Remove null/blank records; fix types (e.g., `ms_played` as integer); deduplicate on business keys (track/artist/album with timestamp window).
* **Feature Engineering**: Mood categories derived from audio features (e.g., valence/energy); behavior flags such as *shuffle*/*skipped*; sessionization by user/time gaps; weekend/weekday flags.
* **Enrichment**: Join external genre/mood catalogs where applicable.
* **Normalization**: Scale numeric features to comparable ranges for clustering/visuals; ensure referential integrity.
* **Validation**: Row counts, null thresholds, type checks, and business rules (e.g., nonnegative `ms_played`).

### 4.3 Load (Snowflake)

* **Schema Strategy**: Star schema with one central fact table and multiple dimensions (see §5).
* **Partitioning/Clustering**: Logical partitioning by year; clustering on date keys and commonly filtered dimensions (genre/sentiment) to improve pruning.
* **Merge/Upsert**: Stage new partitions, then `MERGE` into conformed tables; maintain slowly changing attributes in dimensions where needed.
* **Access**: RBAC roles for read/write, warehouse sizing for cost/perf balance, and query result caching.

### 4.4 Orchestration & Logging

* **AWS Glue Jobs & Crawlers** to infer schemas and catalog tables.
* **CloudWatch** for run logs, metrics, and alerting.
* **Error Handling**: Structured logs, dead‑letter handling for malformed payloads, and reprocess hooks for partial failures.

---

## 5. Data Model (Star Schema)

\*\*Fact: \*\***`fact_streaming_events`**

* Grain: one row per play event.
* Example attributes: `play_id`, `track_id`, `artist_id`, `album_id`, `user_id` (if available), `played_at_utc`, `ms_played`, `popularity`, `device_type`, `is_shuffle`, `is_skipped`.

**Dimensions**

* `dim_track`: track name, ISRC, duration, explicit flag, audio features (tempo, key, mode, danceability, energy, valence, liveness, speechiness, acousticness), release info, links to album/artist.
* `dim_artist`: artist name, canonical artist ID, primary/secondary genres, popularity.
* `dim_album`: album title, label (if available), release date/year, album type.
* `dim_user` (optional when user scope is permitted): user surrogate key, device preferences, region.
* `dim_date`/`dim_time`: calendar attributes (day, week, month, quarter, year), weekend flag, hour‑of‑day bucket.

**Rationale**: The star schema supports quick slice/dice across time, artist/album hierarchies, devices, and mood/genre cohorts while keeping the event grain compact and performant.

---

## 6. Power BI Report Design

**Pages & Navigation**

* **Albums**, **Artists**, **Tracks** pages, with consistent slicers: **Platform/Device** (Android, Mac, Windows), date range, genre/mood.
* Drill‑down from aggregate KPIs → top N lists → detail grids; drillthrough from artist/album to track‑level performance.

**Core Visuals**

* **Trends**: YoY line charts for plays by period with percentage deltas.
* **Top‑N**: Bar/column charts for top artists/albums/tracks.
* **Engagement**: Heatmap of plays by weekday × hour (revealing 18:00–02:00 peaks).
* **Behavior**: Scatter/quad charts contrasting repeat count vs. average listen time; flags for shuffle/skips.
* **Details Grid**: Exportable table for Album/Artist/Track with listening time and filter context preserved.

**Representative DAX Measures** (examples)

* `Plays = COUNTROWS('fact_streaming_events')`
* `Weekend Plays = CALCULATE([Plays], 'dim_date'[IsWeekend] = TRUE())`
* `YoY % = DIVIDE([Plays] - CALCULATE([Plays], SAMEPERIODLASTYEAR('dim_date'[Date])), CALCULATE([Plays], SAMEPERIODLASTYEAR('dim_date'[Date])))`

**Performance Notes**

* Star schema + numeric surrogate keys minimize model size.
* Consider incremental refresh per date partition; pre‑aggregations for heavy visuals.

---

## 7. Analytical Results (2023–2024)

**Albums**

* Total albums played: **3,012**; 2024: **1,824** vs. prior year **2,333** (**–21.82% YoY**).
* 59% of album plays occur on **weekends**; seasonal dip mid‑2023 followed by recovery late‑2024.

**Artists**

* Total artists played: **1,861**; 2024 artist count **–26.39% YoY**.
* The Beatles lead with **1,241 listens**, indicating strong catalog loyalty and cross‑album engagement.

**Tracks**

* Total tracks played: **5,334**; 2024 track count **–11.49%** vs. 2023.
* **60.71%** of track plays happen on **weekends**. Top track: *You Sexy Thing* with **66** plays.

**Listening Patterns**

* Heatmap peak between **18:00–02:00**, strongest on weekends.
* Scatter distribution centers on **10–25** plays per track with **2–4 min** average listen time.
* Behavior filters (*Shuffle*, *Skipped*) enable micro‑segment analysis and quadrant classification (high/low engagement).

---

## 8. Business Value & Use Cases

* **Release Strategy**: Align drops to evening/weekend peaks; tailor promo to high‑engagement windows.
* **Catalog Marketing**: Identify legacy artists with persistent loyalty; re‑surface catalog with themed playlists.
* **Creative Direction**: Relate audio features (e.g., energy/valence) to engagement cohorts to guide A\&R decisions.
* **Paid Media**: Target devices/platforms with higher conversion or completion rates.

---

## 9. Data Quality, Governance & Security

* **DQ Rules**: Mandatory types, null thresholds, duplicate suppression, referential integrity checks.
* **Auditability**: S3 raw zone retention; run metadata (job ID, version, source timestamps) stamped on loads.
* **Security**: Secrets via environment variables/parameter store; Snowflake RBAC for least‑privilege access; column‑level masking where necessary.
* **Monitoring**: CloudWatch metrics & alarms; error budgets and re‑run procedures.

---

## 10. Performance & Cost Optimization

* **Snowflake**: Right‑size virtual warehouses; use result caching; clustering on date/genre; avoid cross‑join explosion; prune via partition keys.
* **ETL**: Batch by date windows; vectorized Pandas ops; pushdown filters; incremental loads only.
* **Power BI**: Incremental refresh; aggregated tables; measure simplification; disable high‑cardinality visuals by default.

---

## 11. Limitations & Assumptions

* API **rate limits** and pagination may bias short windows if not fully backfilled.
* **User‑level** analyses depend on available scope/consent; otherwise, results reflect aggregate behavior.
* Genre and mood **enrichment** rely on external mappings; taxonomy drift is possible.
* Timezone normalization assumes a single canonical timezone for comparability.

---

## 12. Future Enhancements

* Near‑real‑time ingestion via streaming (e.g., Kinesis / Kafka) with micro‑batch upserts.
* **dbt** for modular transformations, tests, and CI/CD.
* Advanced analytics: sequence mining, churn propensity, and uplift modeling.
* Integration of **lyrics sentiment** and social signals to refine mood/engagement models.
* Power BI **deployment pipelines** and RLS for multi‑audience distribution.

---

## 13. How to Run

**Prerequisites**

* Spotify Developer app (Client ID/Secret and redirect URI).
* AWS account with Glue & S3 access; Snowflake account/warehouse.
* Power BI Desktop.

**Setup Steps**

1. **Configure Secrets**: Set environment variables for Spotify and Snowflake in the Glue job.
2. **Provision S3 Buckets**: `/raw/spotify/` for JSON landings; `/curated/` for transformed parquet.
3. **Create Snowflake Objects**: Databases, schemas, stages; run DDL for `dim_*` and `fact_streaming_events`.
4. **Deploy Glue Job**: Package Python code (Spotipy + dependencies); set schedule (daily/weekly) and retries.
5. **Run Crawler**: Update Glue Catalog; verify table schemas.
6. **Load & Validate**: Execute initial backfill; check DQ reports.
7. **Power BI**: Open `Spotify Data Analysis (4).pbix`, configure Snowflake connection, and **Refresh**. Use slicers and drillthrough to explore.

---

## 14. Conclusion

**Spotify Data Analysis** demonstrates a production‑minded pattern for ingesting third‑party API data, curating a governed analytical model, and surfacing insights that inform release timing, promotion, and catalog strategy. The architecture balances scalability, cost, and usability while leaving a clear path to advanced analytics and continuous delivery.

---

## 15. References

* Spotify Web API (developer documentation)
* AWS Glue & CloudWatch (service documentation)
* Snowflake (data warehousing documentation)
* Microsoft Power BI (reporting & DAX documentation)
