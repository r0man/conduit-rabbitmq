(defproject conduit-rabbitmq "0.9.1"
  :description "Conduit Rabbitmq: Conduit Transport for RabbitMQ"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [com.rabbitmq/amqp-client "2.3.1"]
                 [conduit "0.8.1"]]
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [org.clojars.technomancy/clj-stacktrace
                      "0.2.1-20101126.163600-6"]
                     [lein-difftest "1.3.2-20101010.033133-1"
                      :exclusions [clj-stacktrace]]
                     [lein-release "1.1.1"]
                     [lein-fail-fast "1.0.0"]]
  :repositories {"lambda" "http://lambda.sa2s.us/snapshots/"})
