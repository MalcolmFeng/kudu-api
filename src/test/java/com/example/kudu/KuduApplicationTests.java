package com.example.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
class KuduApplicationTests {

    private KuduClient kuduClient;

    private String kuduMaster;

    private String tableName;

    @Before
    public void init() {
        try{
            System.out.println("initing........................");
            //初始化操作
            kuduMaster = "172.22.19.143:7051";
            //指定表名
            tableName = "student";
            KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
            kuduClientBuilder.defaultOperationTimeoutMs(1800000);
            kuduClient = kuduClientBuilder.build();
            System.out.println("服务器地址" + kuduMaster + ":客户端"+ kuduClient +"初始化成功...");
        }catch (Exception e){
            System.out.println(e);
        }
    }

    private static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }
    /**
     * 创建无分区表
     */
    @Test
    public void createTable() throws KuduException {
        init();

        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("id", Type.STRING, true));
        columns.add(newColumn("name", Type.STRING, false));
        columns.add(newColumn("age", Type.INT32, false));
        columns.add(newColumn("sex", Type.INT32, false));
        Schema schema = new Schema(columns);

        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add("id");

        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1);  //设置表的备份数
        options.setRangePartitionColumns(parcols);  //设置range分区
        options.addHashPartitions(parcols, 3);  //设置hash分区和数量
        try {
            kuduClient.createTable("student",schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }finally {
            if (kuduClient != null){
                kuduClient.close();
            }
        }
    }



    /**
     * 向表加载数据
     */
    @Test
    public void insertTable() throws KuduException {
        init();
        //向表加载数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
//        kuduSession.set
        kuduSession.setTimeoutMillis(100000);
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        for (int i = 1; i <= 10; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addString("id", i+"");
            row.addString("name", "zhangsan-" + i);
            row.addInt("age", 20 + i);
            row.addInt("sex", i % 2);
            //最后实现执行数据的加载操作
            kuduSession.apply(insert);
        }
    }




    /**
     * 查询表的数据结果
     */
    @Test
    public void queryData() throws KuduException {
        init();
        //构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");
        columnsList.add("age");
        columnsList.add("sex");
        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        //返回结果集
        KuduScanner kuduScanner = kuduScannerBuilder.build();
        //遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                String id = row.getString("id");
                String name = row.getString("name");
                int age = row.getInt("age");
                int sex = row.getInt("sex");
                System.out.println(">>>>>>>>>>  id=" + id + " name=" + name + " age=" + age + "sex = " + sex);
            }
        }
    }




    /**
     * 修改表的数据
     */
    @Test
    public void updateData() throws KuduException {
        init();
        //修改表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        //Update update = kuduTable.newUpdate();
        //如果 id 存在就表示修改，不存在就新增
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addInt("id", 100);
        row.addString("name", "zhangsan-100");
        row.addInt("age", 100);
        row.addInt("sex", 0);
        //最后实现执行数据的修改操作
        kuduSession.apply(upsert);
    }




    /**
     * 删除数据
     */
    @Test
    public void deleteData() throws KuduException {
        init();
        //删除表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id", 100);
        kuduSession.apply(delete);//最后实现执行数据的删除操作
    }




    @Test
    public void dropTable() throws KuduException {
        init();
        if (kuduClient.tableExists(tableName)) {
            kuduClient.deleteTable(tableName);
        }
    }

}
