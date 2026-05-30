# de-start-sprint-stream-project

Шаг 1. Проверить работу потока.
1. Комманда для kcat(mac)
kcat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="de-student" \
  -X sasl.password="ltcneltyn" \
  -X ssl.ca.location=/Users/ekaterinashulgina/Documents/s8-lessons/streaming-lessons/CA.pem \
  -P \
  -t cybersky_in \
  -K: \
  -l /Users/ekaterinashulgina/Documents/s8-lessons/streaming-lessons/test_data.txt

2.  Команда для проверки, что данные отправлены
  kcat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="de-student" \
  -X sasl.password="ltcneltyn" \
  -X ssl.ca.location=/Users/ekaterinashulgina/Documents/s8-lessons/streaming-lessons/CA.pem \
  -t cybersky_in \
  -C \
  -o beginning

Реализация приложения
Шаг 2. Прочитать данные об акциях из Kafka.
Напишите с помощью PySpark код стриминга для:
 a. чтения сообщений об акциях из Kafka;
 b. вывода сообщений в консоль.
Протестируйте написанный код.

Шаг 2 — readStream из Kafka с SASL/SSL-аутентификацией; 
Шаг 3 — spark.read из Postgres-таблицы subscribers_restaurants через JDBC.
Шаг 4 — from_json десериализует поле value по схеме; фильтр оставляет только активные кампании (datetime_start ≤ now ≤ datetime_stop).
Шаг 5 — inner join потока с Postgres по restaurant_id; добавляется колонка created_at (текущее время); дублирующиеся колонки убраны явным select.
Шаг 6 — функция save_to_postgresql дописывает колонку feedback и сохраняет в subscribers_feedback через JDBC (mode="append").
Шаг 7 — функция send_to_kafka сериализует строки датафрейма в JSON (без feedback) → кладёт в колонку value → пишет в результирующий топик.
Шаг 8 — в foreach_batch_function: сначала df.persist(), затем вызовы обоих стоков, и в блоке finally — df.unpersist(). Стриминг запускается через .foreachBatch(foreach_batch_function).
