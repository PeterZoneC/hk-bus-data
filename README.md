# hk_bus_data

香港公共交通數據爬蟲，為城巴 App 提供 KMB/LWB、NLB、GMB、MTR Bus 的路線、站點、票價及班次數據。

## 輸出文件

| 文件 | 說明 | 大小（約） |
|------|------|---------|
| `bus_data.min.json` | App 下載使用的壓縮 JSON | ~4MB |
| `bus_data.md5` | 用於版本校驗的 MD5 | — |
| `bus_data.json` | 可讀格式（調試用） | ~20MB |

## 數據結構

```json
{
  "version": "20260511110000",
  "generated_at": "2026-05-11T11:00:00+08:00",
  "nlb": {
    "routes": [ { "routeId", "routeNo", "routeName_c/s/e", "overnightRoute", "specialRoute" } ],
    "route_stops": { "<routeId>": [ { "stopId", "stopName_c/s/e", "stopLocation_c/s/e",
                                      "latitude", "longitude", "fare", "fareHoliday",
                                      "someDepartureObserveOnly", "sequence" } ] }
  },
  "kmb": {
    "routes": [ { "route", "bound", "service_type", "orig_en/tc/sc", "dest_en/tc/sc" } ],
    "stops":  { "<stop_id>": { "stop_id", "name_en/tc/sc", "lat", "long" } },
    "route_stops": { "<route|bound|service_type>": ["stop_id", ...] },
    "gtfs_fares": { "<gtfs_route_id>": { "<bound>": ["fare_per_boarding_stop", ...] } },
    "gtfs_freq":  { "<gtfs_route_id>": { "<bound>": { "<calendar>": { "<start_hhmm>": { "end_time", "headway_secs" } } } } }
  },
  "gmb": {
    "routes": [ { "route_id", "route_seq", "region", "route_code",
                  "description_tc/sc/en", "orig_tc/sc/en", "dest_tc/sc/en",
                  "headways": [ { "weekdays"[7], "public_holiday", "start_time",
                                  "end_time", "frequency", "frequency_upper" } ],
                  "stops": ["stop_id", ...],
                  "fares": ["fare", ...] } ],
    "stops": { "<stop_id>": { "lat", "lng" } }
  },
  "mtr_bus": {
    "routes": [ { "route_number", "route_name_chi/eng",
                  "fares": { "adult/child/elderly/joyou/disability/student" × "octopus/single" } } ],
    "stops":  [ { "station_id", "latitude", "longitude",
                  "station_name_chi/eng", "route_number", "direction", "sequence" } ]
  }
}
```

## 數據來源

| 運營商 | 來源 | 票價 | 班次 |
|--------|------|------|------|
| NLB | [rt.data.gov.hk](https://rt.data.gov.hk/v2/transport/nlb/) | API 直接提供（含假日票價）| — |
| KMB/LWB | [data.etabus.gov.hk](https://data.etabus.gov.hk) | GTFS fare_attributes | GTFS frequencies |
| GMB | [data.etagmb.gov.hk](https://data.etagmb.gov.hk) | GTFS fare_attributes | API headways |
| MTR Bus | [opendata.mtr.com.hk](https://opendata.mtr.com.hk) | CSV（6種乘客類別）| — |
| GTFS | [data.gov.hk](https://static.data.gov.hk/td/pt-headway-tc/gtfs.zip) | 補充票價 | 補充班次 |

## 運行

```bash
pip install -r requirements.txt
python main.py
```

## CI / 自動更新

GitHub Actions 每天兩次（09:10 和 21:10 HKT）自動爬取並更新數據。
結果部署到 GitHub Pages，App 通過以下 URL 下載：

```
https://<your-username>.github.io/<repo-name>/bus_data.min.json
```

## App 接入

App 啟動時下載 `bus_data.min.json`（約 4MB），與本地緩存的 MD5 比對，有更新才重新下載解析，存入 SQLDelight 數據庫。
