# Toy TFT for outage recovery.
# Assumes you already have a merged county-hour dataframe with:
# CountyFIPS, datetime, outageFraction, gust_mps, wind_speed_mps,
# precip_mm, pressure_hpa
#
# Goal:
# - start small
# - get one model to fit
# - compare against a baseline
# - produce a quick prediction plot later

import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt

import lightning.pytorch as pl
from lightning.pytorch.callbacks import EarlyStopping, LearningRateMonitor

from pytorch_forecasting import Baseline, TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data import GroupNormalizer
from pytorch_forecasting.metrics import QuantileLoss, MAE

# ------------------------------------------------------------
# 1) Load and prepare data
# ------------------------------------------------------------
df = pd.read_csv("merged_outage_weather.csv")
df["datetime"] = pd.to_datetime(df["datetime"])

# Keep county IDs stable and string-based
df["CountyFIPS"] = df["CountyFIPS"].astype(str).str.zfill(5)
df = df.sort_values(["CountyFIPS", "datetime"]).reset_index(drop=True)

# Toy subset: keep the counties with the most outage activity
toy_counties = (
    df.groupby("CountyFIPS")["outageFraction"]
      .sum()
      .sort_values(ascending=False)
      .head(8)
      .index
)
df = df[df["CountyFIPS"].isin(toy_counties)].copy()

# A real time index in hours from the start of each county series
df["county"] = df["CountyFIPS"]
df["time_idx"] = (
    (df["datetime"] - df.groupby("county")["datetime"].transform("min"))
    .dt.total_seconds() // 3600
).astype(int)

# Optional calendar features
df["hour"] = df["datetime"].dt.hour.astype(int)
df["dayofweek"] = df["datetime"].dt.dayofweek.astype(int)
df["month"] = df["datetime"].dt.month.astype(int)

# Basic cleanup
needed = [
    "county", "time_idx", "outageFraction",
    "gust_mps", "wind_speed_mps", "precip_mm", "pressure_hpa",
    "hour", "dayofweek", "month"
]
df = df.dropna(subset=needed).copy()

print(df[needed].head())
print("Rows:", len(df), "Counties:", df["county"].nunique())

# ------------------------------------------------------------
# 2) Train/validation split
# ------------------------------------------------------------
max_encoder_length = 72   # past 72 hours
max_prediction_length = 12 # forecast next 12 hours

training_cutoff = df["time_idx"].max() - max_prediction_length

training = TimeSeriesDataSet(
    df[df.time_idx <= training_cutoff],
    time_idx="time_idx",
    target="outageFraction",
    group_ids=["county"],

    min_encoder_length=max_encoder_length // 2,
    max_encoder_length=max_encoder_length,
    min_prediction_length=1,
    max_prediction_length=max_prediction_length,

    static_categoricals=["county"],
    time_varying_known_reals=["time_idx", "hour", "dayofweek", "month"],
    time_varying_unknown_reals=[
        "outageFraction",
        "gust_mps",
        "wind_speed_mps",
        "precip_mm",
        "pressure_hpa",
    ],

    target_normalizer=GroupNormalizer(groups=["county"]),
    add_relative_time_idx=True,
    add_target_scales=True,
    add_encoder_length=True,
    allow_missing_timesteps=True,
)

validation = TimeSeriesDataSet.from_dataset(
    training,
    df,
    predict=True,
    stop_randomization=True,
)

batch_size = 64
train_dataloader = training.to_dataloader(train=True, batch_size=batch_size, num_workers=0)
val_dataloader = validation.to_dataloader(train=False, batch_size=batch_size * 2, num_workers=0)

# ------------------------------------------------------------
# 3) Baseline
# ------------------------------------------------------------
baseline = Baseline()
baseline_pred = baseline.predict(val_dataloader, return_y=True)
baseline_mae = MAE()(baseline_pred.output, baseline_pred.y)
print("Baseline MAE:", float(baseline_mae))

# ------------------------------------------------------------
# 4) TFT
# ------------------------------------------------------------
pl.seed_everything(42)

tft = TemporalFusionTransformer.from_dataset(
    training,
    learning_rate=0.03,
    hidden_size=8,
    attention_head_size=1,
    dropout=0.1,
    hidden_continuous_size=8,
    loss=QuantileLoss(),
    log_interval=10,
    reduce_on_plateau_patience=3,
)

print("Model parameters:", tft.size())

trainer = pl.Trainer(
    max_epochs=5,
    accelerator="auto",
    devices="auto",
    gradient_clip_val=0.1,
    enable_checkpointing=False,
    logger=True,
)

trainer.fit(tft, train_dataloader, val_dataloader)

# ------------------------------------------------------------
# 5) Validate
# ------------------------------------------------------------
pred = tft.predict(val_dataloader, return_y=True)
tft_mae = MAE()(pred.output, pred.y)
print("TFT MAE:", float(tft_mae))