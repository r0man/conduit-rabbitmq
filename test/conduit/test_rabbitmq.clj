(ns conduit.test-rabbitmq
  (:use conduit.rabbitmq :reload-all)
  (:use clojure.test
        conduit.core
        arrows.core)
  (:import
     [com.rabbitmq.client ConnectionFactory]))

(defn rabbitmq-connection [host vhost user password]
  "Create a simple rabbitmq connection."
  ;; for rabbitmq client 1.7.2
  #_(let [params (doto (ConnectionParameters.)
                 (.setVirtualHost vhost)
                 (.setUsername "guest")
                 (.setPassword "guest"))
        factory (ConnectionFactory. params)]
    (.newConnection factory "localhost"))

  ;; for rabbitmq client 2.3.1
  (.newConnection
    (doto (ConnectionFactory.)
      (.setHost host)
      (.setVirtualHost vhost)
      (.setUsername user)
      (.setPassword password))))

(declare ^:dynamic *connection*)
(declare ^:dynamic *queue*)

(defn rabbitmq-test-fixture [f]
  (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
              channel (.createChannel connection)]
    (binding [*connection* connection
              *channel* channel
              *exchange* "conduit-exchange"
              *queue* "test-queue"]
      (.exchangeDeclare channel *exchange* "direct")
      (declare-queue *queue*)
      (purge-queue *queue*)
      (f))))

(use-fixtures :each rabbitmq-test-fixture)

(def test-results (atom []))

(def test-rabbit
  (a-rabbitmq "test-queue"
              "test-rabbit"
              (a-arr (fn [x]
                       (swap! test-results conj x)
                       x))))

(def test-fn-rabbit
  (let [queue (atom 0)]
    (a-rabbitmq (fn [_]
                  (swap! queue
                         (fn [x]
                           (if (zero? x) 1 0))))
                #{0 1}
                "test-fn-rabbit"
                (a-arr (fn [x]
                         (swap! test-results conj x)
                         x)))))

(deftest test-rabbit-publish-consume
  (purge-queue *queue*)
  (dorun
   (map (partial publish *queue*)
        (range 50)))

  (is (= (range 50)
         (map #(do
                 (ack-message %)
                 (read-msg %))
              (a-run (msg-stream *queue* 100))))))

(deftest test-rabbitmq-run
  (dorun
   (map (partial publish *queue*)
        (map (fn [x] [(:id test-rabbit) x])
             (range 10))))

  (reset! test-results [])
  (.exchangeDeclare *channel* *exchange* "direct")
  (rabbitmq-run test-rabbit *queue* *channel* *exchange* 100)
  (is (= (range 10)
         @test-results)))

(deftest test-seq-proc
  (let [new-rabbit (a-comp (a-arr inc)
                           test-rabbit)
        new-fn-rabbit (a-comp (a-arr inc)
                              test-fn-rabbit)]
    (conduit-map new-rabbit
                 (range 10))

    (reset! test-results [])
    (.exchangeDeclare *channel* *exchange* "direct")
    (rabbitmq-run new-rabbit *queue* *channel* *exchange* 100)
    (is (= (range 1 11)
           @test-results))

    (declare-queue "0")
    (purge-queue "0")
    (declare-queue "1")
    (purge-queue "1")
    (conduit-map new-fn-rabbit
                 (range 10))

    (reset! test-results [])
    (rabbitmq-run new-fn-rabbit "0" *channel* *exchange* 100)
    (is (= [2 4 6 8 10]
           @test-results))

    (reset! test-results [])
    (rabbitmq-run new-fn-rabbit "1" *channel* *exchange* 100)
    (is (= [1 3 5 7 9]
           @test-results))))

(deftest test-par-proc
  (let [p1 (a-rabbitmq *queue* "a-inc" (a-arr inc))
        p2 (a-rabbitmq *queue* "a-dec" (a-arr dec))
        new-rabbit (a-all p1 p2)
        thread-fn (fn [exchange queue]
                    (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
                                channel (.createChannel connection)]
                      (.exchangeDeclare channel exchange "direct")
                      (rabbitmq-run new-rabbit queue channel exchange)))
        remote-thread (doto (new Thread (partial thread-fn *exchange* *queue*))
                        (.start))]
    (try
      (is (= (map vector
                  (range 1 11)
                  (range -1 9))
             (conduit-map (a-comp new-rabbit
                                  pass-through)
                          (range 10))))
      (finally
       (Thread/sleep 500)
       (.interrupt remote-thread)
       (.join remote-thread 5000)))))

(deftest test-test
  (let [test-proc (a-comp
                   (a-arr inc)
                   (a-rabbitmq "bogus-queue"
                               "a-double"
                               (a-arr (partial * 2))))
        sel-test (a-comp
                  (a-all (a-arr even?)
                         pass-through)
                  (a-select true test-proc
                            false pass-through))
        test-fn (test-conduit-fn test-proc)]

    (is (= [8 10 12 14]
             (mapcat test-fn (range 3 7))))
    (is (= [8 10 12 14]
             (conduit-map (test-conduit test-proc)
                          (range 3 7))))
    (is (= [2 1 6 3 10 5 14]
             (conduit-map (test-conduit sel-test)
                          (range 7))))))
