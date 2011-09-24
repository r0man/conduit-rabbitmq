(defproject conduit-rabbitmq "0.9.3-SNAPSHOT"
  :description "Conduit Rabbitmq: Conduit Transport for RabbitMQ"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.rabbitmq/amqp-client "2.3.1"]
                 [conduit "0.8.6"]]
  ;; for ci purposes
  :repositories {"snapshots" "http://localhost/archiva/repository/snapshots"
                 "releases" "http://localhost/archiva/repository/internal"})
