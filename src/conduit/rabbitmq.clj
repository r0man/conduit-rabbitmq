(ns conduit.rabbitmq
  (:use conduit.core)
  (:import [com.rabbitmq.client Connection ConnectionFactory Channel
            MessageProperties QueueingConsumer]
           [java.util UUID]))

(declare ^:dynamic *channel*)
(declare ^:dynamic *exchange*)

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
    (.basicConsume *channel* queue false consumer)
    consumer))

(defn publish [queue msg]
  (let [msg-str (binding [*print-dup* true]
                  (pr-str msg))]
    (.basicPublish *channel* *exchange* queue
                   (MessageProperties/PERSISTENT_TEXT_PLAIN)
                   (.getBytes msg-str))))

(defn publish-work [source id msg]
  (let [source (if (fn? source)
                 (str (source msg))
                 source)]
    (publish source [id msg])))

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
      (publish-work source id [x reply-queue])
      (let [msg (get-msg reply-queue)]
        (ack-message msg)
        [(read-msg msg) rabbitmq-reply]))))

(defn rabbitmq-sg-fn [source id]
  (fn rabbitmq-reply [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (declare-queue reply-queue true)
      (publish-work source id [x reply-queue])
      (fn []
        (let [msg (get-msg reply-queue)]
          (ack-message msg)
          [(read-msg msg) rabbitmq-reply])))))

(defn rabbitmq-pub-no-reply [source id]
  (fn rabbitmq-no-reply [x]
    (publish-work source id x)
    [[] rabbitmq-no-reply]))

(defn reply-fn [f]
  (partial (fn rabbitmq-reply-fn [f [x reply-queue]]
             (let [[new-x new-f] (f x)]
               (publish reply-queue new-x)
               [[] (partial rabbitmq-reply-fn new-f)]))
           f))

(defn a-rabbitmq
  ([source id proc]
     (if (fn? source)
       (throw (Exception. (str "Return values for 'source' function "
                               "must be supplied."))))
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
  ([source-fn sources id proc]
     (let [sources (set (map str sources))
           id (str id)
           reply-id (str id "-reply")
           part {id (:no-reply proc)
                 reply-id (reply-fn (:reply proc))}
           new-parts (into {}
                           (map #(vector % part)
                                sources))]
       {:type :rabbitmq
        :created-by (:created-by proc)
        :args (:args proc)
        :source source-fn
        :id id
        :reply (rabbitmq-pub-reply source-fn reply-id)
        :no-reply (rabbitmq-pub-no-reply source-fn id)
        :scatter-gather (rabbitmq-sg-fn source-fn reply-id)
        :parts (merge-with merge (:parts proc)
                           new-parts)})))

(defn msg-stream [queue & [msecs]]
  (let [consumer (consumer queue)]
    (fn stream-reader [x]
      (try
        (let [msg (if msecs
                    (.nextDelivery consumer msecs)
                    (.nextDelivery consumer))]
          (when msg
            [[msg] stream-reader]))
        (catch InterruptedException e
          nil)))))

(def ^:dynamic *conduit-rabbitmq-id* nil)

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
