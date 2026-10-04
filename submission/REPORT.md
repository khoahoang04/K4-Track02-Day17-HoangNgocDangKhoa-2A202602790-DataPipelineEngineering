# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Hoàng Ngọc Đăng Khoa / 2A202602790
**Repo:** https://github.com/khoahoang04/K4-Track02-Day17-HoangNgocDangKhoa-2A202602790-DataPipelineEngineering
**Commit bài nộp:** HEAD (commit mới nhất trên branch main)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity CLI (Gemini 3.8 Flash) hỗ trợ phân tích triệu chứng lỗi và rà soát mã nguồn pipeline.
**Nguồn tham khảo khác (nếu có):** Slide Day 17 (Data Pipeline Engineering), tài liệu dbt microbatch & merge.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `scripts/verify.py` fail: `silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)`, T-91 có 3 hàng thay vì trạng thái cuối; `gold_doc_chunks` sinh 22 hàng / 9 chunks. | `scripts/verify.py` fail: `gold_feature_daily reconciles with a full recompute (c50b8851affe != 8630e04a61d1)`, u05 chỉ có 2 events thay vì 5 events; C1 khác C0 khi rerun. | `scripts/verify.py` fail: `deleted ticket T-97 is a tombstone` nhận được bản ghi chưa xoá (`is_deleted=False`), T-97 còn nguyên trong snapshot `v2026-08-16` và RAG chunks. |
| **Nguyên nhân gốc** | `pipeline/silver.py::upsert_silver_tickets` dùng câu lệnh `INSERT INTO` thuần túy, chỉ chèn thêm dòng mới của từng batch thay vì keyed upsert; chạy lại batch cũ ghi đè/nhân bản dòng. | `pipeline/config.py` thiết lập `LOOKBACK_DAYS = 0` (chỉ xử lý đúng ngày nạp), khiến batch ngày 08-15 không recompute lại partition 08-12 theo event time cho các event đến trễ của u05. | `pipeline/staging.py::ticket_changes_sql` chỉ lấy `ticket_id` từ `after->>'ticket_id'`. Khi Debezium phát sinh event xoá (`op = 'd'`), trường `after` là `null` nên `ticket_id` bị null và bị loại bỏ bởi mệnh đề WHERE. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: đổi `INSERT` thành `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3` tương ứng với giá trị `ceil(p99)` đo đạc từ Bronze (`3.00` ngày), giúp cửa sổ tính toán lùi lại 3 ngày để nhận trọn vẹn late events. | `pipeline/staging.py`: trích xuất `ticket_id` bằng `coalesce(after->>'ticket_id', before->>'ticket_id', key->>'ticket_id')`, kết hợp logic MERGE để cập nhật tombstone (`is_deleted=True`, PII null) và lan xoá xuống Gold. |
| **Khái niệm trên slide** | Silver — Có khoá; MERGE theo khoá; Bốn cách viết idempotent; LSN guard (newer state wins). | Data về muộn; Event time vs Ingest time; Lookback window = ceil(P99); Overwrite-partition idempotent. | CDC log-based; Debezium envelope (before/after/op); Tombstone chống hồi sinh; Xoá phải lan (Delete propagation). |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE theo khoá kèm điều kiện LSN giúp bảng thực thể duy nhất và không bị trạng thái cũ ghi đè khi replay, trong khi overwrite-partition cho phép tính lại toàn bộ chỉ số trong cửa sổ lookback mà không lo trùng lặp dữ liệu tổng hợp.
- Tombstone thay vì xoá hẳn hàng trong Silver: Tombstone lưu vết xoá và LSN cao nhất nhằm ngăn chặn batch cũ replay làm hồi sinh dữ liệu (ghost record), đồng thời tuân thủ bảo mật bằng cách xoá sạch (NULL) các trường dữ liệu cá nhân.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Giữ tính bất biến tuyệt đối của dữ liệu huấn luyện để tái lập kết quả mô hình (reproducibility) và tránh rò rỉ thông tin tương lai, còn dữ liệu phản hồi đến muộn sẽ được ghi nhận vào snapshot của phiên bản ngày mới hơn.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Quy mô dữ liệu vừa và nhỏ vận hành in-process trên DuckDB đem lại tốc độ thực thi mili-giây, chi phí phần cứng và vận hành tối thiểu, không gặp overhead mạng/JVM của cụm Spark phân tán mà vẫn đảm bảo đầy đủ ACID và SQL chuẩn.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - Mâu thuẫn: Tính bất biến (Immutability) nhằm phục vụ kiểm toán và tái lập mô hình ML, trong khi quyền được lãng quên (GDPR Right to be Forgotten) bắt buộc không được lưu trữ thông tin cá nhân của người dùng đã rút lui.
   - Giải pháp:
     + Tách biệt PII bằng Crypto-shredding: Lưu văn bản dưới dạng token hoá / mã hoá với khoá riêng của từng user/ticket. Khi nhận yêu cầu xoá, hệ thống chỉ cần huỷ khoá mã hoá; văn bản trong các snapshot lịch sử lập tức trở thành chuỗi vô nghĩa không thể giải mã, vừa bảo vệ quyền riêng tư vừa giữ nguyên tính toàn vẹn cấu trúc của snapshot.
     + Quy trình Rewrite có kiểm toán (Audited Compaction): Thiết lập pipeline đặc biệt để viết lại các snapshot cũ (loại bỏ T-97), cập nhật lineage/checksum mới và lưu biên bản kiểm toán pháp lý giải trình lý do thay đổi snapshot.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - Chốt PII: Triển khai mô hình NLP nhận diện thực thể định danh (Named Entity Recognition - NER như PhoBERT-NER / spaCy / Microsoft Presidio) để phát hiện thực thể `PERSON`, kết hợp đối soát với bảng danh mục định danh khách hàng (Customer Master Data).
   - Tầng đặt: Đặt tại ranh giới chuyển tiếp giữa Bronze và Silver (trong Quality Gate / Staging transform) để đảm bảo dữ liệu khi cập bến Silver đã sạch PII, ngăn PII lan rộng xuống các tầng phân tích và huấn luyện downstream.
   - Cách đo: Đánh giá độ chính xác chốt chặn trên bộ dữ liệu kiểm thử vàng (Golden holdout set) bằng Precision/Recall (ưu tiên Recall cao để hạn chế sót PII); định kỳ lấy mẫu ngẫu nhiên (Audit Sampling) trên Silver/Gold tính tỷ lệ rò rỉ PII (Leakage Rate = 0).

## 5. Output (dán nguyên văn)

```text
PS > .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS > .\.venv\Scripts\python.exe -m pytest
..................................                                                                        [100%]
34 passed in 2.52s

PS > .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS > .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS > $env:DO_NOT_TRACK = '1'; Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
15:32:45  Running with dbt=1.12.5
15:32:45  Registered adapter: duckdb=1.11.0
15:32:46  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
15:32:46  
15:32:46  Concurrency: 1 threads (target='dev')
15:32:46  
15:32:46  1 of 19 START sql view model main.stg_events ................................... [RUN]
15:32:46  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
15:32:46  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
15:32:46  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
15:32:46  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
15:32:46  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
15:32:46  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
15:32:47  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
15:32:47  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
15:32:47  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.13s]
15:32:47  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
15:32:47  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
15:32:47  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
15:32:47  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
15:32:47  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
15:32:47  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
15:32:47  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
15:32:47  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
15:32:47  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
15:32:47  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
15:32:47  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
15:32:47  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
15:32:47  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
15:32:47  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
15:32:47  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
15:32:47  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
15:32:47  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
15:32:47  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
15:32:47  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15:32:47  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
15:32:47  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
15:32:47  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
15:32:47  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
15:32:47  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
15:32:47  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.25s]
15:32:47  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
15:32:47  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
15:32:47  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
15:32:47  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
15:32:47  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
15:32:47  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
15:32:47  
15:32:47  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.25 seconds (1.25s).
15:32:47  
15:32:47  Completed successfully
15:32:47  
15:32:47  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS > .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

# BONUS B1 — LLM Step
PS > .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
