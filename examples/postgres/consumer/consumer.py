from kafka import KafkaConsumer
from json import loads
import sys
import psycopg2
import psycopg2.extras


# if sys.__name__ == '__main__':

# # PostgreSQL
# class DemoConsumer(object):
#     last_message = None

#     def __call__(self, msg):
#         print(msg.payload)
#         self.last_message = msg
#         msg.cursor.send_feedback(flush_lsn=msg.data_start)


# democonsumer = DemoConsumer()

# conn = psycopg2.connect(
#     "host=localhost dbname=postgres user=postgres password=postgres",
#     connection_factory=psycopg2.extras.LogicalReplicationConnection,
# )
# cur = conn.cursor()
# try:
#     # test_decoding produces textual output
#     cur.start_replication(slot_name="pytest", decode=True)
# except psycopg2.ProgrammingError:
#     cur.create_replication_slot("pytest", output_plugin="test_decoding")
#     cur.start_replication(slot_name="pytest", decode=True)

# print("Starting streaming, press Control-C to end...", file=sys.stderr)
# try:
#     cur.consume_stream(democonsumer)
# except KeyboardInterrupt:
#     cur.close()
#     conn.close()
#     print(
#         "The slot 'pytest' still exists. Drop it with "
#         "SELECT pg_drop_replication_slot('pytest'); if no longer needed.",
#         file=sys.stderr,
#     )
#     print(
#         "WARNING: Transaction logs will accumulate in pg_xlog "
#         "until the slot is dropped.",
#         file=sys.stderr,
#     )

# cur.drop_replication_slot("pytest")

# Kafka

consumer = KafkaConsumer(
    "dbserver1.inventory.customers",
    bootstrap_servers=["kafka:9092"],
    # auto_offset_reset="earliest",
    group_id=None,
)

print("Listening")
for message in consumer:
    print(message.value)
