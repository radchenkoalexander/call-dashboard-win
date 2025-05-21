import pandas as pd
import os
from dotenv import load_dotenv
import boto3

# ========================
# 1. Загрузка и обработка данных
# ========================

print("Начинаем обработку данных...")

df = pd.read_csv("calls_raw_full.csv")

# Работаем с датами
df['call_date'] = df['call_date'].astype('datetime64[ns]')
df['date'] = df['call_date'].dt.date
df['year_month'] = df['call_date'].dt.to_period("M").astype(str)

# Фильтруем (убираем слишком короткие звонки, и строки с пустыми датами)
df = df[(df['duration_sec'] >= 40) & (~df['call_date'].isna())]

# Новый столбец для вычисления среднего количества разговора в день
df['duration_hour'] = round(df['duration_sec'] / 3600, 2)

# Бины по времени для определения активности менеджера в течение суток
bins = [8, 11, 14, 16, 19]
labels = ["09-11 (Утро)", "11-14 (До обеда)", "14-16 (После обеда)", "16-18 (Конец дня)"]
df["call_hour"] = df["call_date"].dt.hour
df["time_bin"] = pd.cut(df["call_hour"], bins=bins, labels=labels, right=True)

# Переименовываем типы звонков
df['direction_type'] = df['direction_type'].replace({
    'internal': 'Внутренний',
    'in': 'Входящий',
    'out': 'Исходящий'
})

# Счетчик для агрегаций
df['count'] = 1

# Агрегация 1: по типам звонков
call_by_type = df.groupby(['manager', 'direction_type', 'year_month'])['count'].count().reset_index()

# Агрегация 2: по временным бинам
call_by_time_bin = df.groupby(['manager', 'time_bin', 'year_month'], observed=True)['count'].count().reset_index()

# Агрегация 3: средняя продолжительность звонков в часах
avg_call_duration = df.groupby(['manager', 'year_month']).duration_hour.sum().reset_index()
avg_call_duration['duration_hour'] = avg_call_duration['duration_hour'] / 23
avg_call_duration['duration_hour'] = avg_call_duration['duration_hour'].round(2).astype(float)

# Сохраняем CSV локально
call_by_type.to_csv('call_by_type.csv', index=False)
call_by_time_bin.to_csv('call_by_time_bin.csv', index=False)
avg_call_duration.to_csv('avg_call_duration.csv', index=False)

# ========================
# 2. Отправка в Yandex Cloud
# ========================

load_dotenv()

bucket = os.getenv("YC_BUCKET_NAME")
key = os.getenv("YC_ACCESS_KEY")
secret = os.getenv("YC_SECRET_KEY")

s3 = boto3.client(
    service_name='s3',
    region_name='ru-central1',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=key,
    aws_secret_access_key=secret
)

# Просто загружаем файлы
print("Загружаем файлы в облако...")

s3.upload_file("call_by_type.csv", bucket, "call_by_type.csv")
s3.upload_file("call_by_time_bin.csv", bucket, "call_by_time_bin.csv")
s3.upload_file("avg_call_duration.csv", bucket, "avg_call_duration.csv")

input("\nНажмите Enter, чтобы выйти...")
