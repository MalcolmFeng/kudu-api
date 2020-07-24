package com.example.kudu.kudu;

import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 具体使用请见test.java.com.example.kudu
 */
public class Kudu {

    public static Kudu INSTANCE = new Kudu();

    private Kudu() {
        init();
        addShutdownHook();
    }

    private KuduClient client = null;
    private Map<String, KuduTable> tables = new HashMap<>();
    private Logger logger = LoggerFactory.getLogger(Kudu.class);

    private void init() {
        client = new KuduClient.KuduClientBuilder("192.168.1.132").defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(30000).defaultAdminOperationTimeoutMs(60000).build();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        logger.error("ShutdownHook Close KuduClient Error!", e);
                    }
                }
            }
        });
    }

    public KuduClient client() {
        return client;
    }

    public KuduTable table(String name) throws KuduException {
        KuduTable table = tables.get(name);
        if (table == null) {
            table = client.openTable(name);
            tables.put(name, table);
        }
        return table;
    }

    /**
     * FlushMode:AUTO_FLUSH_BACKGROUND
     *
     * @return
     * @throws KuduException
     */
    public KuduSession newAsyncSession() throws KuduException {
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setFlushInterval(500);
        session.setMutationBufferSpace(5000);
        return session;
    }

    /**
     * FlushMode:AUTO_FLUSH_SYNC
     *
     * @return
     * @throws KuduException
     */
    public KuduSession newSession() throws KuduException {
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        session.setMutationBufferSpace(5000);
        return session;
    }

    public void closeSession(KuduSession session) {
        if (session != null && !session.isClosed()) {
            try {
                session.close();
            } catch (KuduException e) {
                logger.error("Close KuduSession Error!", e);
            }
        }
    }

    public KuduScanner.KuduScannerBuilder scannerBuilder(String table) {
        return client.newScannerBuilder(tables.get(table));
    }
}
