# -*- coding: utf-8 -*-
import os, json, requests, pandas as pd, pytz, pathlib
from datetime import datetime
from dateutil import tz
BINANCE_FAPI = "https://fapi.binance.com"
SYMBOLS = os.environ.get("SYMBOLS","ARBUSDT,BTCUSDT,ETHUSDT").split(",")
BARS = int(os.environ.get("BARS","60"))
OUTDIR = os.environ.get("OUTDIR","out")
TZ = tz.gettz("Asia/Seoul")
def kst_iso_from_ms(ts_ms:int)->str:
    dt = datetime.utcfromtimestamp(ts_ms/1000).replace(tzinfo=pytz.UTC).astimezone(TZ)
    return dt.isoformat()
def get_klines(symbol, interval="15m", limit=60)->pd.DataFrame:
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/klines",
                     params={"symbol":symbol,"interval":interval,"limit":limit}, timeout=15)
    r.raise_for_status()
    rows=[]
    for o in r.json():
        rows.append({"ts_kst": kst_iso_from_ms(o[6]),
                     "open":float(o[1]),"high":float(o[2]),"low":float(o[3]),"close":float(o[4]),
                     "volume":float(o[5]),"quote_volume":float(o[7]),"trades":int(o[8])})
    return pd.DataFrame(rows)
def get_oi(symbol)->dict:
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/openInterest", params={"symbol":symbol}, timeout=10)
    r.raise_for_status(); return {"oi": float(r.json()["openInterest"])}
def get_mark_funding(symbol)->dict:
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex", params={"symbol":symbol}, timeout=10)
    r.raise_for_status(); d=r.json()
    return {"mark_price":float(d["markPrice"]),
            "funding_rate":float(d.get("lastFundingRate",0.0)),
            "next_funding_time_kst": kst_iso_from_ms(d["nextFundingTime"]) if "nextFundingTime" in d else None}
def main():
    pathlib.Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    now_kst = datetime.now(TZ).isoformat()
    for sym in SYMBOLS:
        kl = get_klines(sym,"15m",BARS)
        kl.to_csv(f"{OUTDIR}/01_{sym}_15m_{BARS}.csv", index=False)
        panel = {"symbol":sym,"ts_kst":now_kst, **get_oi(sym), **get_mark_funding(sym)}
        with open(f"{OUTDIR}/panel_{sym}.json","w",encoding="utf-8") as f:
            json.dump(panel,f,ensure_ascii=False)
if __name__=="__main__": main()
