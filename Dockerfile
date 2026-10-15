FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg aria2 fonts-dejavu-core util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 -d /home/bot bot
# يوتيوب محتاج JS runtime لحل التوقيعات
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno
ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    DB_PATH=/data/bot.db
WORKDIR /app
COPY requirements.txt .
# tgcrypto ملهوش wheels جاهزة (بيتبني من المصدر) فلازم gcc وقت البناء بس، وبنشيله بعد التسطيب
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN mkdir -p /data && chown -R 10001:10001 /data
# sed: لو الملف اتعدّل على Windows (نهايات أسطر CRLF) ما يبوظش الـ entrypoint
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh
# الـ entrypoint بيحدّث المحركات ويجهّز /data ثم يشغّل البوت بمستخدم غير root
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "bot.py"]
