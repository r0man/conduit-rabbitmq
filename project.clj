(defproject conduit-rabbitmq "0.9.2-SNAPSHOT"
  :description "Conduit Rabbitmq: Conduit Transport for RabbitMQ"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [com.rabbitmq/amqp-client "2.3.1"]
                 [conduit "0.8.5"]]
  ;; for ci purposes
  :repositories {"snapshots" "http://localhost/archiva/repository/snapshots"
                 "releases" "http://localhost/archiva/repository/internal"})
