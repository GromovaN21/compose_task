# 1. Клонирование репозитория
git clone https://github.com/GromovaN21/compose_task
cd compose_task

# 2. Создать .env 
cp .env.example .env

# 3. Запустить
docker compose up -d

# 4. Проверить статус контейнеров
docker compose ps
