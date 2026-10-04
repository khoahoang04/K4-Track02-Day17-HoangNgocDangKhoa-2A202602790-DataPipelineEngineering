# K4-Track02-Day17-Data-Pipeline-Engineering

> 🇬🇧 English version: [`README_en.md`](README_en.md)

**Hình thức: bài cá nhân.** Dùng chung cho K4, Track 02, Ngày 17.
Tên repo đề bài: `K4-Track02-Day17-Data-Pipeline-Engineering`.

Dựng đường ống dữ liệu cho **nền tảng AI hỗ trợ khách hàng** — đúng bài toán
xuyên suốt của slide Ngày 17 — rồi **sửa ba lỗi đã cài sẵn** và chứng minh
pipeline chạy lại được bằng **checksum**.

```
Postgres tickets ── Debezium CDC ──┐
Kafka support.events ──────────────┼─▶ Bronze ─────────▶ Silver ──────────────┬─▶ gold_doc_chunks    → RAG index
S3 transcripts (JSON) ─────────────┘   Parquet,          MERGE theo khoá,     ├─▶ gold_training_set  → classifier
                                       bất biến          PII đã che           └─▶ gold_feature_daily → routing agent
```

Mọi thứ chạy **zero-key, đa nền tảng** trên DuckDB + Python. Không cần Docker,
không cần cloud. Track dbt dùng chính Bronze đó.

Sơ đồ trên mô tả hệ thống nguồn của bài toán. Trong lab, Postgres/CDC, Kafka và S3
được mô phỏng bằng file JSON/JSONL trong `data/`; không chạy các dịch vụ nguồn thật.
`gold_doc_chunks` dùng vector hash 16 chiều để kiểm tra chunk/cache, không phải
embedding ngữ nghĩa hay một hệ thống RAG hoàn chỉnh. Regex PII chỉ che email và
số điện thoại; tên người và nội dung snapshot cũ được bàn ở câu hỏi suy ngẫm.

## Mục tiêu học tập

Sau bài lab, bạn cần giải thích và kiểm chứng được:

- Cam kết của Bronze, Silver và Gold, cùng cách đọc CDC Debezium.
- Ghi dữ liệu theo khoá, chọn trạng thái mới nhất và truyền thao tác xoá xuống Gold.
- Đo lateness từ Bronze và xử lý dữ liệu đến muộn theo event time.
- Snapshot training theo thời điểm, cache embedding và checksum khi chạy lại.
- Đối chiếu kết quả của pipeline Python với các model dbt.

## Chuẩn bị và tài liệu

Cần Python **3.10+**, Git, terminal và tài khoản GitHub để nộp repo public.
Đọc được Python cơ bản, SQL `JOIN`, `GROUP BY` và window functions.
Phần dbt cần cài thêm `requirements-dbt.txt`; Docker chỉ cần nếu chọn bonus Airflow.

| Tài liệu | Nội dung |
|---|---|
| [SUBMISSION.md](docs/SUBMISSION.md) | Tên repo bài nộp, file cần nộp, deadline và checklist |
| [RUBRIC.md](docs/RUBRIC.md) | Tiêu chí chấm: 100 điểm bắt buộc, bonus tối đa 10 điểm |
| [CHECKPOINTS.md](docs/CHECKPOINTS.md) | Các mốc thực hành, kiến thức và cách tự kiểm tra |
| [RULES.md](docs/RULES.md) | Sử dụng AI, hợp tác, nộp muộn và bảo mật |
| [VIBE-CODING.md](docs/VIBE-CODING.md) | Gợi ý làm việc với AI coding agent |

Tài liệu hướng dẫn nằm trong `docs/`, các đề bonus trong `docs/bonus/`.
Mã pipeline nằm trong `pipeline/`; tiện ích kiểm tra và sinh seed nằm trong `scripts/`.
Chạy lệnh từ thư mục gốc repo. Các tiện ích dùng dạng `python -m scripts.<tên>`;
ví dụ `python -m scripts.verify`. Các target `make` vẫn dùng như dưới đây.

```text
K4-Track02-Day17-Data-Pipeline-Engineering/
├── README.md, README_en.md  # Điểm bắt đầu
├── main.py                 # Chạy pipeline
├── pipeline/               # Bronze, Silver, Gold và điều phối
├── scripts/                # Verify, rerun, parity, bonus LLM, sinh seed
├── docs/                   # Nộp bài, rubric, checkpoint, quy định, hướng dẫn AI
│   └── bonus/              # Đề brainstorm tiếng Việt và tiếng Anh
├── tests/                  # Unit tests và contracts
├── data/                   # Dữ liệu seed
├── dbt_project/            # Model SQL và cấu hình dbt
├── docker/                 # Airflow bonus
├── extensions/             # Flywheel và knowledge graph
└── submission/             # REPORT và checksum bài nộp
```

---

## Nhiệm vụ (2,5 giờ)

Repo này **cố tình có 3 lỗi**. Clone về chạy `make verify` sẽ thấy `FAILURES`.

1. Chạy pipeline, đọc các check bị fail, **tìm 3 lỗi** trong `pipeline/`.
2. **Sửa** chúng — không sửa `scripts/verify.py`, `tests/`, `data/` hay cách tính checksum.
3. Chứng minh: `make rerun3` → chạy lại ngày **2026-08-12 ba lần**, ba checksum Gold
   **giống hệt nhau và giống bản build mới** (`submission/checksums.txt`).
4. Track dbt: `make dbt` pass, `make parity` → hai cách cài đặt cho cùng một checksum.
5. Viết `submission/REPORT.md` (phần phân tích ≤ 1 trang, không tính output): mỗi lỗi — triệu chứng, nguyên nhân gốc,
   cách sửa, khái niệm trên slide; mỗi lựa chọn công cụ — vì sao.

Gợi ý phân bổ: 20' đọc code + chạy · 75' ba lỗi · 25' dbt · 30' report.

---

## Bắt đầu nhanh

### Trên Windows PowerShell (Khuyến nghị)

```powershell
# 1. Khởi tạo môi trường ảo và cài đặt thư viện
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -r requirements-dbt.txt

# 2. Chạy pipeline và kiểm thử
.\.venv\Scripts\python.exe main.py                 # build mới: reset Silver/Gold, backfill 08-10 .. 08-16 từ Bronze
.\.venv\Scripts\python.exe -m scripts.verify       # 18 contract — bản clone về sẽ FAIL, đó chính là bài lab
.\.venv\Scripts\python.exe -m pytest               # pytest (34 unit & contract tests)
.\.venv\Scripts\python.exe -m scripts.rerun_check  # BÀI KIỂM TRA CHẤM ĐIỂM: chạy lại 2026-08-12 ba lần
.\.venv\Scripts\python.exe main.py --lateness      # đo độ trễ của event từ Bronze (P50 / P95 / P99)
```

### Trên macOS/Linux (hoặc shell có `make`)

```bash
make setup        # tạo .venv + cài requirements.txt
make run          # build mới: reset Silver/Gold, backfill 08-10 .. 08-16 từ Bronze
make verify       # 18 contract — bản clone về sẽ FAIL, đó chính là bài lab
make test         # pytest
make rerun3       # BÀI KIỂM TRA CHẤM ĐIỂM: chạy lại 2026-08-12 ba lần
make lateness     # đo độ trễ của event từ Bronze (P50 / P95 / P99)
```

Bản seed chưa sửa sẽ có check fail; đây là kết quả dự kiến của đề bài. Toàn bộ bài lab đã được kiểm thử và tương thích 100% trên Windows với Python 3.11.4, dbt-core 1.12.5 và dbt-duckdb 1.11.0.

---

## Có gì trong repo

| File | Tầng | Làm gì | Slide |
|---|---|---|---|
| `data/` | nguồn | Thứ các hệ thống nguồn giao mỗi ngày: CDC Debezium (dạng Kafka record), Kafka events, transcript export | Bài toán xuyên suốt |
| `pipeline/bronze.py` | Bronze | Land mỗi (nguồn, ngày) thành **một file Parquet bất biến**; land lại = không làm gì | Bronze — cam kết |
| `pipeline/staging.py` | Bronze→ | Parse **phong bì Debezium** (`before`/`after`/`op`/`lsn`, tombstone); cần sửa xử lý delete | CDC log-based |
| `pipeline/quality.py` | gate | **Pydantic** kiểm từng event; sai → `quarantine_events`, không dừng run | Kiểm thử dữ liệu |
| `pipeline/silver.py` | Silver | Ticket hiện tại, lịch sử **SCD2**, events, transcripts và che PII; cần sửa ghi ticket thành upsert theo khoá | Silver — có khoá |
| `pipeline/gold.py` | Gold | `gold_feature_daily` (theo **event time**, có **lookback**), `gold_training_set` (**snapshot có version**, point-in-time), `gold_doc_chunks` (cache embedding theo **hash + model version**) | Gold — đúng hình dạng |
| `pipeline/run.py`, `pipeline/dag.py` | điều phối | Một DAG cho một ngày; backfill = **cùng code path**, từng ngày | Chạy lại & backfill |
| `pipeline/checksum.py` | chấm | Checksum không phụ thuộc thứ tự dòng (SQL thuần, tự chạy được trong DuckDB CLI) | Bài kiểm tra cuối cùng |
| `scripts/rerun_check.py` | chấm | Build mới → chạy lại một ngày cũ 3 lần → so checksum | Lab 17 |
| `dbt_project/` | dbt | Hai bảng chung `silver_tickets`, `gold_feature_daily`: `merge` + `merge_update_condition`, `microbatch` + `lookback`, contract, unit test | dbt, Microbatch |
| `docker/` | bonus | Cùng daily run trên **Airflow 3** thật (`airflow.sdk`, `airflow backfill create`) | Airflow 2 → 3 |
| `pipeline/llm_label.py` | bonus | Bước **LLM gán nhãn** ticket — bản ngây thơ, bạn thêm cache theo hash | LLM là một bước transform |
| `extensions/` | mở rộng | Flywheel trace → eval/DPO và knowledge graph, không chấm điểm | — |

---

## Dữ liệu seed: những câu chuyện được cài sẵn

Bảy ngày 2026-08-10 → 2026-08-16, đủ nhỏ để đọc bằng mắt
(`scripts/generate_seed.py` sinh lại toàn bộ):

Các ngày trong dữ liệu seed là ngày mô phỏng, **không phải ngày học hay deadline**.

- **T-91** tạo `low/open` ngày 08-10 → `high` ngày 08-14 → `closed/bug` ngày 08-16
  (đúng ví dụ trên slide Silver). Thay đổi ngày 08-14 bị Kafka **giao hai lần**.
- **T-97** chứa tên, email, số điện thoại; đóng ngày 08-12; **bị xoá** ngày 08-15
  (yêu cầu xoá dữ liệu). Debezium gửi `op = 'd'` với `after = null`, rồi một tombstone.
- **u05** ở trên tàu, mất mạng tối 08-12: click và một 👎 cho T-88 tới Kafka
  **ngày 08-15** — trễ 3 ngày.
- Ngày 08-13 consumer khởi động lại: 2 event bị giao lại. Có 2 event hỏng
  (rating `meh`, thiếu `user_id`).

---

## Bài kiểm tra chấm điểm: ba checksum

Slide: *"Chạy lại một ngày cũ ba lần liên tiếp, ghi checksum bảng Gold sau mỗi lần.
Ba con số phải giống hệt nhau."* `make rerun3` làm đúng như vậy, và chặt hơn một bước:

```
fresh build             C0   ← reset Silver/Gold, backfill mọi ngày từ Bronze
re-run #1 of 2026-08-12 C1
re-run #2 of 2026-08-12 C2
re-run #3 of 2026-08-12 C3   PASS ⇔ C0 = C1 = C2 = C3
```

Vì sao phải bằng **C0** chứ không chỉ C1 = C2 = C3? Một pipeline có thể "ổn định
sai": lần chạy lại đầu tiên làm hỏng dữ liệu, các lần sau hỏng y như vậy. Ba con số
giống nhau mà khác bản build mới vẫn là FAIL.

---

## Gợi ý khi bí (mở từng tầng, đừng mở hết một lúc)

<details><summary>Lỗi ở Silver — <code>silver_tickets</code> có nhiều hàng cho một ticket</summary>

Đọc lại slide *"Silver — Có khoá"* và *"Bốn cách viết idempotent"*. Một hàng = một
thực thể cần **khoá**. Rồi hỏi tiếp: chạy lại batch **cũ** sau batch **mới** thì
trạng thái nào phải thắng? Cột nào cho bạn biết thay đổi nào mới hơn?
</details>

<details><summary>Lỗi ở Gold — <code>gold_feature_daily</code> không khớp full recompute</summary>

Chạy `make lateness`. Slide *"Data về muộn"*: lookback đặt bằng P99 của
`(_ingested_at − event_time)` — **đo từ Bronze, đừng đoán**.
</details>

<details><summary>Lỗi xoá — T-97 vẫn còn ở Silver, training set, RAG index</summary>

Mở `data/cdc/tickets/2026-08-15.jsonl` và nhìn bản ghi `op = "d"`. Khoá của ticket
nằm ở đâu khi `after` là `null`? Slide *"CDC log-based"* và *"Xoá phải lan"*.
</details>

---

## Track dbt (có chấm)

**Trên Windows PowerShell:**

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dbt.txt
.\.venv\Scripts\python.exe main.py --land-only
$env:DO_NOT_TRACK = '1'
Push-Location dbt_project
try {
    ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
} finally {
    Pop-Location
}
.\.venv\Scripts\python.exe -m scripts.parity
```

**Trên macOS/Linux (Makefile):**

```bash
make setup-dbt
make dbt          # land Bronze → dbt build: PASS=19 (models + data tests + unit test)
make parity       # silver_tickets + gold_feature_daily: lite vs dbt cùng checksum
```

`dbt_project/` triển khai hai bảng dùng cho parity, không bao gồm SCD2, transcripts,
quarantine, training snapshot hay doc chunks của pipeline Python. `silver_tickets` là
`incremental_strategy='merge'` với `unique_key` và `merge_update_condition` theo LSN,
`gold_feature_daily` là `microbatch` (`batch_size='day'`, `lookback=3`), có contract,
`data_tests:` và một **unit test** cho logic dedup + xoá. Nếu `scripts.parity` báo
MISMATCH thì một trong hai bản đang sai — thường là bản bạn chưa sửa xong.

---

## Bonus (tối đa +10, không bắt buộc)

- **B1 — Bước LLM có cache** (+5): `pipeline/llm_label.py` gọi LLM cho mọi ticket
  ở mọi lần chạy và ghi bất cứ thứ gì model trả về. Chạy kiểm tra:
  - Windows PowerShell: `.\.venv\Scripts\python.exe -m scripts.bonus_llm`
  - macOS/Linux: `make bonus-llm`
  Kết quả in `BONUS PASS`: khoá cache = hash(input) + model + prompt version, chạy lại 0 lần
  gọi, đổi prompt thì gắn nhãn lại có chủ đích, output sai schema → quarantine.
  Zero-key: `FakeLLM` thay cho model thật.
- **B2 — chọn một** (+5): chạy daily run trên **Airflow 3** (`make docker-up`, rồi
  xem [hướng dẫn Airflow](docs/AIRFLOW.md), chụp 7 run và checksum), **hoặc** phiên brainstorm
  bài toán thật trong [`BONUS-CHALLENGE.md`](docs/bonus/BONUS-CHALLENGE.md).

Làm cả B1 và B2 được cộng tối đa 10 điểm. Hai lựa chọn trong B2 không cộng dồn.
Bonus là điểm cộng bài lab; bỏ qua bonus không làm giảm điểm bắt buộc.

## Mở rộng (không chấm)

`make flywheel` (agent traces → eval set + cặp DPO, decontamination, ASOF join) và
`make kg` (knowledge graph vs vector retrieval), xem
[`extensions/README.md`](extensions/README.md).

---

## Nộp bài

Mỗi học viên nộp **một URL repo GitHub public** vào ô bài tập K4 / Track 02 / Day 17
trên LMS; không nộp bằng PR. Tên repo bài nộp:
`K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`.

Deadline mặc định: **23:59 ngày diễn ra lab, múi giờ Asia/Ho_Chi_Minh (UTC+7)**,
trừ khi key coach thông báo điều chỉnh. Xem [SUBMISSION.md](docs/SUBMISSION.md) để biết
file phải nộp, cách kiểm tra và quy định chốt bài; tiêu chí chấm nằm trong [RUBRIC.md](docs/RUBRIC.md).

Mới làm việc cùng AI coding agent? Đọc [`VIBE-CODING.md`](docs/VIBE-CODING.md) trước —
và nhớ: bạn phải giải thích được từng dòng mình sửa trong REPORT.

Định dạng bảng lakehouse mà Bronze/Gold sẽ hạ cánh là **Ngày 18**; feature store /
vector DB mà Gold nuôi là **Ngày 19**; observability và lineage cho pipeline này là
**Ngày 27**.
