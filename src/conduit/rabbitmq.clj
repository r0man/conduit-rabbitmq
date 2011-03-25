(ns conduit.rabbitmq
  (:use conduit.core)
  (:import [com.rabbitmq.client Connection ConnectionFactory Channel
            MessageProperties QueueingConsumer]
           [java.util UUID]))

(declare *channel*)
(declare *exchange*)

(defmacro conduit-rabbitmq [channel exchange & body]
  `(binding [*channel* ~channel
             *exchange* ~exchange]
     ~@body))

(defn declare-queue
  ([queue]
     (declare-queue queue false))
  ([queue autodelete]
     (.queueDeclare *channel* queue true false autodelete {})
     (.queueBind *channel* queue *exchange* queue)))

(defn purge-queue [queue]
  (.queuePurge *channel* queue))

(defn consumer [queue]
  (let [consumer (QueueingConsumer. *channel*)]
    (.basicConsume *channel* queue, false, consumer)
    consumer))

(defn publish [queue msg]
  (let [msg-str (binding [*print-dup* true]
                  (pr-str msg))]
    (.basicPublish *channel* *exchange* queue
                   (MessageProperties/PERSISTENT_TEXT_PLAIN)
                   (.getBytes msg-str))))

(defn get-msg
  ([queue]
     (try
       (.nextDelivery (consumer queue))
       (catch InterruptedException e
         nil)))
  ([queue msecs] (.nextDelivery (consumer queue) msecs)))

(defn read-msg [m]
  (read-string (String. (.getBody m))))

(defn ack-message [msg]
  (.basicAck *channel*
             (.getDeliveryTag (.getEnvelope msg))
             false))

(defn rabbitmq-pub-reply [source id]
  (fn rabbitmq-reply [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (declare-queue reply-queue true)
      (publish source [id [x reply-queue]])
      (let [msg (get-msg reply-queue)]
        (ack-message msg)
        [(read-msg msg) rabbitmq-reply]))))

(defn rabbitmq-sg-fn [source id]
  (fn rabbitmq-reply [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (declare-queue reply-queue true)
      (publish source [id [x reply-queue]])
      (fn []
        (let [msg (get-msg reply-queue)]
          (ack-message msg)
          [(read-msg msg) rabbitmq-reply])))))

(defn rabbitmq-pub-no-reply [source id]
  (fn rabbitmq-no-reply [x]
    (publish source [id x])
    [[] rabbitmq-no-reply]))

(defn reply-fn [f]
  (partial (fn rabbitmq-reply-fn [f [x reply-queue]]
             (let [[new-x new-f] (f x)]
               (publish reply-queue new-x)
               [[] (partial rabbitmq-reply-fn new-f)]))
           f))

(defn a-rabbitmq [source id proc]
  (let [source (str source)
        id (str id)
        reply-id (str id "-reply")]
    {:type :rabbitmq
     :created-by (:created-by proc)
     :args (:args proc)
     :source source
     :id id
     :reply (rabbitmq-pub-reply source reply-id)
     :no-reply (rabbitmq-pub-no-reply source id)
     :scatter-gather (rabbitmq-sg-fn source reply-id)
     :parts (merge-with merge (:parts proc)
                        {source {id (:no-reply proc)
                                 reply-id (reply-fn (:reply proc))}})}))

(defn msg-stream [queue & [msecs]]
  (let [consumer (consumer queue)]
    (if msecs
      (fn this-fn1 [x]
        (let [msg (.nextDelivery consumer msecs)]
          (when msg
            [[msg] this-fn1])))
      (fn this-fn2 [x]
        (try
          (let [msg (.nextDelivery consumer)]
            [[msg] this-fn2])
          (catch InterruptedException e
            nil))))))

(def *high-priority-timeout* 1000)

(defn priority-msg-stream [queues & [msecs]]
  (let [consumers (for [[q w] (sort-by second queues)] (consumer q))]
    (if msecs
      (fn this-fn1 [x]
        (try
          (loop [[c & cs] consumers]
            (let [msg (.nextDelivery c msecs)]
              (cond
               msg [[msg] this-fn1]
               (seq cs) (recur cs)
               :else nil)))
          (catch InterruptedException e
            nil)))
      (fn this-fn2 [x]
        (try
          (loop [[c & cs] consumers wait *high-priority-timeout*]
            (let [msg (.nextDelivery c (long wait))]
              (cond
               msg [[msg] this-fn2]
               (seq cs) (recur cs (/ wait 2))
               :else (recur consumers *high-priority-timeout*))))
          (catch InterruptedException e
            nil))))))

(def *conduit-rabbitmq-id* nil)

(defn msg-handler-fn [f msg]
  (let [[id arg] (read-msg msg)
        [_ new-f] (binding [*conduit-rabbitmq-id* id]
                    (f [id arg]))]
    (ack-message msg)
    [[] (partial msg-handler-fn new-f)]))

(defn rabbitmq-run [p queue channel exchange & [msecs]]
  (when-let [handler-map (get-in p [:parts queue])]
    (binding [*channel* channel
              *exchange* exchange]
      (let [queue (str queue)
            select-handler (partial select-fn handler-map)
            handler-fn (comp-fn (msg-stream queue msecs)
                                (partial msg-handler-fn
                                         select-handler))]
        (declare-queue queue)
        (dorun (a-run handler-fn))))))

(defn rabbitmq-priority-run [p queues channel exchange & [msecs]]
  (when-let [handler-map (apply merge-with
                                merge
                                (map #(get-in p [:parts %]) (keys queues)))]
    (binding [*channel* channel
              *exchange* exchange]
      (let [select-handler (partial select-fn handler-map)
            handler-fn (comp-fn (priority-msg-stream msg-stream queues msecs)
                                (partial msg-handler-fn
                                         select-handler))]
        (declare-queue queue)
        (doseq [[q _] queues]
          (declare-queue q))
        (dorun (a-run handler-fn))))))
