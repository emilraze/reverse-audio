import os
import tempfile
import uuid
from io import BytesIO
import random

import boto3
from flask import Flask, request, jsonify
from pydub import AudioSegment
from pytube import YouTube

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_video():
    data = request.get_json()
    youtube_link = data['link']
    chunk_duration = int(data['chunkDuration'])
    chunks = int(data['chunks'])

    # Скачиваем только аудио
    yt = YouTube(youtube_link)
    audio_stream = yt.streams.get_audio_only()
    audio_path = audio_stream.download()
    audio = AudioSegment.from_file(audio_path)

    # Длина отрезка в миллисекундах
    chunk_length = chunk_duration * 1000

    audio_duration = len(audio)

    # Разделяем аудио рандомно на нужное количество отрезков
    chunked_audio = []
    for _ in range(chunks):
        start_time = random.randint(0, audio_duration - chunk_length)
        chunk = audio[start_time:start_time + chunk_length]
        chunked_audio.append(chunk)

    # Реверсим каждый отрезок
    reversed_chunks = [chunk.reverse() for chunk in chunked_audio]

    # Сохраняем в S3
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
                      aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
                      endpoint_url='https://storage.yandexcloud.net',
                      region_name='ru-central1')

    reversed_urls = []
    # Создаём временный файл для каждого готрезка
    for i, chunk in enumerate(reversed_chunks):
        with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as temp_file:
            chunk.export(temp_file.name, format="mp3")
            temp_file.seek(0)
            byte_content = temp_file.read()

        # Генерируем рандомный UUID и сохраняем в S3, а также записываем в ответ
        file_name = f'{uuid.uuid4()}.mp3'
        byte_content_io = BytesIO(byte_content)
        s3.upload_fileobj(byte_content_io, 'songs', file_name)
        reversed_urls.append({"index": i, "url": file_name})

        # Удаляем временный файл
        os.remove(temp_file.name)

    # Удаляем оригинальный файл
    os.remove(audio_path)

    return jsonify({"videoTitle": yt.title, "reversedSongPartDtos": reversed_urls})

if __name__ == '__main__':
    app.run(debug=True)