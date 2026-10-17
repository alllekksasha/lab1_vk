# Отчёт по практической работе №1

**Тема:** Построение Lakehouse с архитектурой Bronze / Silver / Gold  
**Вариант:** Q16 — Parts/Supplier Relationship

## 1. Вариант и масштаб

| Поле | Значение |
| --- | --- |
| ФИО (как в VK Education) | **Бодрова Александра** |
| Вариант (запрос) | **Q16 — Parts/Supplier Relationship** |
| Масштаб `sfN` | **sf1** |
| Почему выбрала такой масштаб | Я выбрала `sf1`, потому что он позволяет проверить работу полноценного Lakehouse на моём компьютере без слишком долгой загрузки и обработки. Исходный набор содержит 200 000 деталей (`part`), 800 000 связей деталей и поставщиков (`partsupp`) и 10 000 поставщиков (`supplier`). Этого достаточно для соединений, фильтрации, агрегации, проверки корректности Gold и сравнения GZIP с ZSTD. |

## 2. Поднятые сервисы — и почему именно они

| Сервис | Роль в стеке | Почему выбрала именно его (а не альтернативу) |
| --- | --- | --- |
| **Silo (`pgsty/silo`, S3-совместим с MinIO)** | Объектное хранилище для Parquet-файлов в бакетах `bronze`, `silver`, `gold`. | Я выбрала локальное S3-совместимое хранилище, чтобы не платить за облачные ресурсы и работать с тем же интерфейсом S3 API, который используется в облачных Lakehouse. |
| **PostgreSQL 17** | Хранит служебные метаданные Lakekeeper. | Я выбрала PostgreSQL как свободную и надёжную СУБД, с которой работает Lakekeeper. Она отделяет метаданные каталога от файлов данных. |
| **Lakekeeper** | Iceberg REST Catalog: управляет warehouse, пространствами имён и метаданными Iceberg-таблиц. | Я выбрала Lakekeeper, поскольку он поддерживает открытый формат Apache Iceberg и позволяет работать со снапшотами и изменением схемы, не привязывая данные к конкретному SQL-движку. |
| **Trino 476** | SQL-движок: обращается к Lakekeeper за метаданными, читает и записывает данные в S3 и получает исходный TPC-H. | Я выбрала Trino из-за поддержки Iceberg и TPC-H, SQL-запросов и `EXPLAIN ANALYZE` для измерения производительности. Не понадобилось устанавливать отдельную СУБД для аналитических таблиц. |
| **Metabase** | BI-инструмент: подключается к Trino и показывает таблицы и визуализации Bronze. | Я выбрала Metabase за простой веб-интерфейс и подключение к Trino без программирования собственного приложения. В лабораторной я проверила просмотр таблицы `part` и её агрегатов. |

Я запускала контейнеры в Docker Desktop на Windows из PowerShell. Команды ниже отражают использованную локальную конфигурацию. Для производственной среды демонстрационные пароли и отключённая авторизация не подходят.

### Скрипты, которыми поднимала стек

```powershell
# Создание общей сети для контейнеров
docker network create lakehouse-net

# Локальное S3-совместимое хранилище (Silo)
docker run -d --name lakehouse-minio --network lakehouse-net -p 9000:9000 -p 9011:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin123 -v minio-data:/data pgsty/silo:latest server /data --console-address ":9001"

# PostgreSQL для метаданных Lakekeeper
docker run -d --name lakehouse-pg --network lakehouse-net -e POSTGRES_PASSWORD=postgres -p 5432:5432 -v pg-data:/var/lib/postgresql/data postgres:17

# Подготовка схемы метаданных Lakekeeper
docker run --rm --network lakehouse-net -e "LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!" -e "LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakehouse-pg:5432/postgres" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakehouse-pg:5432/postgres" quay.io/lakekeeper/catalog:latest-main migrate

# Запуск каталога Iceberg REST
docker run -d --name lakekeeper --network lakehouse-net -e "LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!" -e "LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@lakehouse-pg:5432/postgres" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@lakehouse-pg:5432/postgres" -p 8181:8181 quay.io/lakekeeper/catalog:latest-main serve

# Подключение встроенного генератора TPC-H к Trino
"connector.name=tpch" | Set-Content -Path "catalog\tpch.properties" -Encoding ascii

# Конфигурации трёх Iceberg-каталогов в Trino
foreach ($n in @("bronze", "silver", "gold")) {
@"
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog
iceberg.rest-catalog.warehouse=$n
iceberg.rest-catalog.security=NONE
fs.native-s3.enabled=true
s3.endpoint=http://lakehouse-minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin123
"@ | Set-Content -Path "catalog\$n.properties" -Encoding ascii
}

# SQL-движок Trino 476
docker run -d --name trino --network lakehouse-net -p 8080:8080 -v "${PWD}\catalog:/etc/trino/catalog" trinodb/trino:476

# Дополнительный BI-инструмент Metabase
docker run -d --name metabase --network lakehouse-net -p 3000:3000 -e MB_DB_FILE=/metabase-data/metabase.db -v metabase-data:/metabase-data metabase/metabase:latest

# Проверка работающих контейнеров
docker ps
```

В интерфейсе Silo (`http://localhost:9011`) я вручную создала бакеты `bronze`, `silver`, `gold`. В Lakekeeper (`http://localhost:8181/ui/`) создала одноимённые Warehouse: для каждого указала нужный бакет, регион `us-east-1`, endpoint `http://lakehouse-minio:9000`, ключи доступа и включила **Path style access**. Без этой опции валидация не проходила из-за ошибки DNS при обращении к бакету. После её включения проверки чтения, записи и удаления тестовых файлов прошли успешно. Команда `SHOW CATALOGS` в Trino показала `bronze`, `silver`, `gold`, `system`, `tpch`.

## 3. Разбор запроса — какие данные нужны

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает |
| --- | --- | --- |
| `tpch.sf1.part` | `partkey`, `brand`, `type`, `size` | `partkey` — соединение с `partsupp`; `brand`, `type`, `size` — фильтры; `brand`, `type`, `size` — группировка и вывод. |
| `tpch.sf1.partsupp` | `partkey`, `suppkey` | `partkey` — соединение с `part`; `suppkey` — проверка поставщика и `COUNT(DISTINCT suppkey)`. |
| `tpch.sf1.supplier` | `suppkey`, `comment` | По `comment LIKE '%Customer%Complaints%'` определяю поставщиков с жалобами; через `suppkey` исключаю их из результатов. |

Для Q16 я исключила бренд `Brand#45`, типы, начинающиеся с `MEDIUM POLISHED`, и оставила размеры `(49, 14, 23, 45, 19, 3, 36, 9)`. Затем исключила поставщиков с жалобами и рассчитала количество **уникальных** поставщиков для каждой комбинации бренда, типа и размера. Другие колонки исходных таблиц мне не нужны.

## 4. Медальон — решения по каждому слою

Я использовала таблицы Apache Iceberg в формате Parquet. Так как масштаб `sf1` небольшой, партиционирование не применяла: для этих объёмов оно усложнило бы структуру хранения и могло создать много маленьких файлов. В Bronze я сохранила все строки исходных трёх таблиц, выбрав только нужные Q16 колонки (то есть это сырой слой по строкам, но не полная копия всех исходных колонок). В Silver выполнила фильтрацию и соединения, а в Gold — агрегацию.

**Примечание к размерам:** в таблицах указаны результаты чтения `file_size_in_bytes` из системных таблиц Iceberg `"<table>$files"`, переведённые в МиБ (1 МиБ = 1 048 576 байт). Это размер файлов **данных**, без служебных метаданных Iceberg. Значения округлены; для небольших таблиц использованы более точные значения в КиБ. В форме преподавателя колонка называется «МБ», но технически измерение производилось в МиБ.

### Bronze (сырой)

| Таблица | Колонки (что оставила) | Строк | Вес, МиБ | Формат | Сжатие | Партиционирование | Обоснование |
| --- | --- | ---: | ---: | --- | --- | --- | --- |
| `bronze.q16.part` | `partkey`, `brand`, `type`, `size` | 200 000 | 0,7 | PARQUET | GZIP | Нет | Я оставила все строки деталей и только поля, нужные Q16. Parquet удобен для выборочного чтения колонок; GZIP применён как первый тестируемый кодек. Партиционирование на `sf1` не требуется. |
| `bronze.q16.partsupp` | `partkey`, `suppkey` | 800 000 | 2,2 | PARQUET | GZIP | Нет | Я сохранила все связи деталей и поставщиков для соединения. Parquet эффективно хранит два числовых поля, GZIP позволяет оценить экономию места. Из-за небольшого объёма не делила таблицу на партиции. |
| `bronze.q16.supplier` | `suppkey`, `comment` | 10 000 | 0,2 | PARQUET | GZIP | Нет | Комментарии нужны для поиска жалоб. Я сохранила исходные комментарии без фильтрации, чтобы правило исключения применялось только на следующем слое. Отдельное партиционирование небольшой таблицы нецелесообразно. |

**Команды создания Bronze:**

```powershell
# Схема для исходных данных
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS bronze.q16"

# Исходные детали, Parquet + GZIP
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.q16.part WITH (format = 'PARQUET') AS SELECT partkey, brand, type, size FROM tpch.sf1.part"

# Связи деталей и поставщиков, Parquet + GZIP
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.q16.partsupp WITH (format = 'PARQUET') AS SELECT partkey, suppkey FROM tpch.sf1.partsupp"

# Поставщики с исходными комментариями, Parquet + GZIP
docker exec trino trino --session bronze.compression_codec=GZIP --execute "CREATE TABLE bronze.q16.supplier WITH (format = 'PARQUET') AS SELECT suppkey, comment FROM tpch.sf1.supplier"
```

В Trino 476 кодек я задавала через параметр сеанса `--session bronze.compression_codec=GZIP`, поскольку свойство таблицы `compression_codec` в `WITH (...)` в моей конфигурации не распознавалось.

### Silver (очищенный)

| Таблица | Колонки (что оставила) | Строк | Вес, МиБ | Формат | Сжатие | Партиционирование | Обоснование |
| --- | --- | ---: | ---: | --- | --- | --- | --- |
| `silver.q16.good_suppliers` | `suppkey` (позже добавила `status`) | 9 996 | 0,0182 (18,6 КиБ) | PARQUET | ZSTD | Нет | Я исключила четырёх поставщиков, в комментариях которых найдены жалобы. Для дальнейшего соединения нужен только ключ `suppkey`. ZSTD выбрала для компактного хранения очищенной таблицы, а партиционирование при 9 996 строках лишнее. |
| `silver.q16.part_supplier` | `brand`, `type`, `size`, `suppkey` | 118 274 | 0,4 | PARQUET | ZSTD | Нет | Я соединила детали с поставками и допустимыми поставщиками, применив фильтры Q16. Таблица содержит только необходимые для итоговой агрегации поля. ZSTD экономит место, а отсутствие партиций упрощает обработку. |

**Команды создания Silver:**

```powershell
# Схема очищенных данных
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS silver.q16"

# Поставщики без жалоб
docker exec trino trino --session silver.compression_codec=ZSTD --execute "CREATE TABLE silver.q16.good_suppliers WITH (format = 'PARQUET') AS SELECT suppkey FROM bronze.q16.supplier WHERE comment NOT LIKE '%Customer%Complaints%'"

# Соединённые и отфильтрованные детали / поставщики
docker exec trino trino --session silver.compression_codec=ZSTD --execute "CREATE TABLE silver.q16.part_supplier WITH (format = 'PARQUET') AS SELECT p.brand, p.type, p.size, ps.suppkey FROM bronze.q16.partsupp ps JOIN bronze.q16.part p ON p.partkey = ps.partkey JOIN silver.q16.good_suppliers s ON ps.suppkey = s.suppkey WHERE p.brand <> 'Brand#45' AND p.type NOT LIKE 'MEDIUM POLISHED%' AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)"
```

### Gold (витрина)

| Таблица | Колонки | Строк | Вес, МиБ | Формат | Сжатие | Партиционирование | Обоснование |
| --- | --- | ---: | ---: | --- | --- | --- | --- |
| `gold.q16.supplier_counts` | `brand`, `type`, `size`, `supplier_cnt` | 18 314 | 0,0438 (44,8 КиБ) | PARQUET | SNAPPY | Нет | Я вынесла подсчёт уникальных поставщиков в отдельную готовую витрину. SNAPPY выбрала как быстрое сжатие для чтения аналитических результатов. При таком небольшом размере партиционирование не принесло бы пользы. |

**Команды создания и проверки Gold:**

```powershell
# Схема аналитических витрин
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS gold.q16"

# Итоговая витрина Q16, Parquet + SNAPPY
docker exec trino trino --session gold.compression_codec=SNAPPY --execute "CREATE TABLE gold.q16.supplier_counts WITH (format = 'PARQUET') AS SELECT brand, type, size, COUNT(DISTINCT suppkey) AS supplier_cnt FROM silver.q16.part_supplier GROUP BY brand, type, size"

# Десять строк итогового результата
docker exec trino trino --execute "SELECT brand, type, size, supplier_cnt FROM gold.q16.supplier_counts ORDER BY supplier_cnt DESC, brand, type, size LIMIT 10"
```

Я проверила совпадение Gold с исходным Q16 с помощью разности множеств `EXCEPT` в обе стороны. Получила **0 расхождений**, то есть итоговая витрина эквивалентна исходному запросу для использованного набора данных.

```sql
WITH expected AS (
    SELECT p.brand, p.type, p.size,
           COUNT(DISTINCT ps.suppkey) AS supplier_cnt
    FROM tpch.sf1.partsupp ps
    JOIN tpch.sf1.part p ON p.partkey = ps.partkey
    WHERE p.brand <> 'Brand#45'
      AND p.type NOT LIKE 'MEDIUM POLISHED%'
      AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
      AND ps.suppkey NOT IN (
          SELECT suppkey FROM tpch.sf1.supplier
          WHERE comment LIKE '%Customer%Complaints%'
      )
    GROUP BY p.brand, p.type, p.size
), differences AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM gold.q16.supplier_counts)
    UNION ALL
    (SELECT * FROM gold.q16.supplier_counts EXCEPT SELECT * FROM expected)
)
SELECT COUNT(*) AS differences_count FROM differences;
-- Результат: 0
```

## 5. Тесты производительности

Для эксперимента я создала второй набор Bronze в схеме `bronze.q16_zstd` с теми же данными `tpch.sf1`, но со сжатием ZSTD. Исходные таблицы со сжатием GZIP сохранила в `bronze.q16`. Для измерения выполняла одинаковый запрос Q16 через `EXPLAIN ANALYZE` и брала значение `Execution`; размеры считала как сумму `file_size_in_bytes` файлов трёх таблиц Bronze.

| Сжатие на Bronze | Время запроса | Размер Bronze, МиБ | Мой вывод (что лучше и почему) |
| --- | --- | --- | --- |
| **GZIP** | 2,08 с; 1,27 с; 1,16 с. **Среднее 1,50 с** | **3,1** | GZIP дал небольшой объём файлов. На повторных запусках запрос выполнялся быстрее первого, поэтому единичный замер не отражает стабильной скорости. |
| **ZSTD** | 1,51 с; 1,22 с; 1,64 с. **Среднее 1,46 с** | **3,0** | ZSTD занял немного меньше места и показал близкое среднее время. Я выбрала бы ZSTD для Bronze из-за небольшого выигрыша в объёме, но утверждать, что он стабильно быстрее, по трём запускам нельзя. |

Средние значения посчитаны по трём запускам каждого кодека. Если исключить первые прогоны как прогревочные, среднее составит **1,22 с для GZIP** и **1,43 с для ZSTD**. Это подтверждает, что на `sf1` разница по скорости мала и нестабильна, а результаты зависят от прогрева, кешей и текущей нагрузки. Разница в размере по округлённым данным — примерно 0,1 МиБ (около 3,2%).

**Создание сравнительного набора ZSTD:**

```powershell
# Отдельная схема для эксперимента ZSTD
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS bronze.q16_zstd"

# Те же исходные таблицы, но другой кодек
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.q16_zstd.part WITH (format = 'PARQUET') AS SELECT partkey, brand, type, size FROM tpch.sf1.part"
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.q16_zstd.partsupp WITH (format = 'PARQUET') AS SELECT partkey, suppkey FROM tpch.sf1.partsupp"
docker exec trino trino --session bronze.compression_codec=ZSTD --execute "CREATE TABLE bronze.q16_zstd.supplier WITH (format = 'PARQUET') AS SELECT suppkey, comment FROM tpch.sf1.supplier"
```

**Пример запроса измерения (для каждой версии Bronze заменяла имя схемы):**

```sql
EXPLAIN ANALYZE
SELECT p.brand, p.type, p.size,
       COUNT(DISTINCT ps.suppkey) AS supplier_cnt
FROM bronze.q16.partsupp ps
JOIN bronze.q16.part p ON p.partkey = ps.partkey
WHERE p.brand <> 'Brand#45'
  AND p.type NOT LIKE 'MEDIUM POLISHED%'
  AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps.suppkey NOT IN (
      SELECT suppkey
      FROM bronze.q16.supplier
      WHERE comment LIKE '%Customer%Complaints%'
  )
GROUP BY p.brand, p.type, p.size
ORDER BY supplier_cnt DESC, p.brand, p.type, p.size;
```

## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Мой ответ |
| --- | --- |
| **Зачем нужны большие данные и Lakehouse (а не обычная БД/хранилище)?** | Я использовала Lakehouse, чтобы разделить дешёвое масштабируемое объектное хранение файлов и вычисления. В отличие от простого набора файлов в S3, Apache Iceberg добавляет метаданные таблиц, снапшоты, изменение схемы и согласованное управление версиями. Для небольшого `sf1` хватило бы обычной БД, но Lakehouse демонстрирует подход, который удобнее масштабировать и использовать с разными движками. |
| **Пример Schema Evolution на Silver: добавила/переименовала колонку — что произошло?** | Я выполнила `ALTER TABLE silver.q16.good_suppliers ADD COLUMN status VARCHAR`. Команда завершилась сообщением `ADD COLUMN`. После этого в таблице осталось **9 996 строк**, а `COUNT(status)` оказался равен **0**: для прежних строк новое поле содержит `NULL`. Данные не пришлось пересоздавать и загружать повторно. |
| **Пример скрипта восстановления данных из снапшота (Time Travel)** | Я получила идентификатор исходного снапшота **`9205729315061316154`** и прочитала старую версию таблицы с помощью `FOR VERSION AS OF`. Затем вставила тестовую запись `suppkey = -1`, увидела новую версию с 9 997 строками, а старую — с 9 996. После демонстрации тестовую запись удалила обычным `DELETE`; это вернуло текущие данные к 9 996 строкам, но **не являлось откатом метаданных на старый снапшот**. Скрипт чтения и пример восстановления данных приведены ниже. |

**Выполненные команды Schema Evolution:**

```sql
ALTER TABLE silver.q16.good_suppliers ADD COLUMN status VARCHAR;
SELECT COUNT(*) AS total_rows, COUNT(status) AS filled_status
FROM silver.q16.good_suppliers;
-- Результат: 9996, 0
```

**Time Travel — действия, которые я реально выполнила:**

```sql
-- Получила идентификаторы версий
SELECT snapshot_id, committed_at, operation
FROM silver.q16."good_suppliers$snapshots"
ORDER BY committed_at;

-- Прочитала исходную версию: 9996 строк
SELECT COUNT(*)
FROM silver.q16.good_suppliers
FOR VERSION AS OF 9205729315061316154;

-- Создала новую версию с одной тестовой строкой
INSERT INTO silver.q16.good_suppliers (suppkey, status)
VALUES (-1, 'TEST');

-- Сравнила текущие и исторические данные: 9997 против 9996
SELECT 'current' AS version, COUNT(*) AS rows_count
FROM silver.q16.good_suppliers
UNION ALL
SELECT 'old_snapshot' AS version, COUNT(*) AS rows_count
FROM silver.q16.good_suppliers
FOR VERSION AS OF 9205729315061316154;

-- Убрала тестовую строку из текущей версии
DELETE FROM silver.q16.good_suppliers WHERE suppkey = -1;
-- Проверка: 9996 строк, тестовых строк 0
```

**Пример скрипта восстановления содержимого из снапшота** :

```sql
-- Сохраняю содержимое старого снапшота во временную таблицу
CREATE TABLE silver.q16.good_suppliers_restore AS
SELECT suppkey, status
FROM silver.q16.good_suppliers
FOR VERSION AS OF 9205729315061316154;
```




### 6.2. Подключить инструмент к Lakehouse

| Инструмент | Почему выбрала именно его |
| --- | --- |
| **Metabase** | Я выбрала Metabase как простой BI-инструмент, который позволяет открыть таблицы Bronze через Trino и сразу получить табличное представление и базовые графики. Для задачи просмотра данных он оказался удобнее, чем запуск дополнительного тяжёлого вычислительного движка вроде Spark. |

**Скрипт развёртывания:**

```powershell
# Запуск Metabase в общей Docker-сети
docker run -d --name metabase --network lakehouse-net -p 3000:3000 -e MB_DB_FILE=/metabase-data/metabase.db -v metabase-data:/metabase-data metabase/metabase:latest
```

**Мои действия в интерфейсе:** я открыла `http://localhost:3000`, завершила первоначальную настройку, выбрала драйвер **Starburst (Trino)** и создала подключение `Lakehouse Q16 Bronze`: хост `trino`, порт `8080`, каталог `bronze`, имя пользователя `admin`, без SSL и без пароля (для локальной конфигурации Trino без аутентификации). Подключение прошло успешно, синхронизация завершилась. В Metabase появились таблицы `Part`, `Partsupp` и `Supplier` из схемы `q16`. Я открыла карточку таблицы `Part` и получила показатель **Total Part = 200 000**, а также визуализации распределения деталей по ключу и размеру. Это подтвердило не только подключение к каталогу, но и фактическое чтение Bronze через Trino.

## 7. Выводы

В этой работе я построила локальный Lakehouse на основе S3-совместимого хранилища, Lakekeeper, PostgreSQL и Trino и разделила обработку Q16 на слои Bronze, Silver и Gold. Я убедилась, что Bronze сохраняет исходные строки, Silver отвечает за очистку и соединения, а Gold позволяет быстро получать готовое количество поставщиков для каждой группы деталей; сравнение с исходным запросом дало **0 расхождений**. На масштабе `sf1` ZSTD занял немного меньше места, чем GZIP, но устойчивого преимущества по времени выполнения выявить не удалось. Я также проверила Schema Evolution, чтение исторического снапшота Iceberg и подключила Metabase для просмотра данных Bronze. Для этой задачи я считаю выбранную архитектуру удобной благодаря разделению хранения, управления метаданными и выполнения аналитических запросов.
