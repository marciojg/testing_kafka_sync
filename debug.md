docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic pix_requests --from-beginning

docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic pix_requests --from-beginning --max-messages 5


docker compose exec backend-consumer sh -c \
  'ruby -e "require \"./karafka\"; Karafka.producer.produce_sync(topic: \"pix_requests\", payload: %{{payer:\"cli\",payee:\"asd\",amount:10,clientId:\"manual-2\",correlationId:\"manual-2\",responseSubject:\"pix.responses.manual-2.manual-2\"}.to_json})"'


docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic pix_requests <<'EOF'
{"payer":"cli","payee":"asd","amount":1,"description":"teste","clientId":"manual-1","correlationId":"manual-1","responseSubject":"pix.responses.manual-1.manual-1"}
EOF
