package com.example.kudu;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

public class KuduTest {


    public static void main(String[] args) throws KuduException {
        final String master = "test03";
        KuduClient client = new KuduClient.KuduClientBuilder(master).build();
        //打开表
        KuduTable sparrow_test = client.openTable("sparrow_test");

        scanTable(client, sparrow_test);
        System.out.println("*************************************");
        insetRow(client,sparrow_test);
        System.out.println("==============================================");
        updateTable(client, sparrow_test);
        scanTable(client, sparrow_test);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
        deleteRow(client,sparrow_test);
        scanTable(client, sparrow_test);

        searchRowWithRange(client, sparrow_test);
        searchWithCondition(client, sparrow_test);
    }

    //更改表数据
    public static void updateTable(KuduClient client, KuduTable table) throws KuduException {
        KuduSession kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        row.addInt("id", 1);
        row.addString("name", "cheng");
        kuduSession.apply(update);
        kuduSession.flush();
        kuduSession.close();
    }

    //插入数据
    public static void insetRow(KuduClient client, KuduTable table) throws KuduException {
        KuduSession kuduSession = client.newSession();
        //设置手动刷新
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        kuduSession.setMutationBufferSpace(3000);

        double v = new Random().nextDouble();
        for (int i = 1; i < 10; i++) {
            Insert insert = table.newInsert();
            insert.getRow().addInt("id", i);
            insert.getRow().addString("name", i + "号");
            insert.getRow().addDouble("score", Double.parseDouble(new DecimalFormat("#.0").format(new Random().nextDouble() * 100)));
            kuduSession.flush();
            kuduSession.apply(insert);
        }
        kuduSession.close();
    }

    //遍历整张表
    public static void scanTable(KuduClient client, KuduTable table) throws KuduException {
        //创建scanner
        KuduScanner scanner = client.newScannerBuilder(table).build();
        //遍历数据
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                System.out.println("scan table: >>>>>>>" + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getDouble(2));
            }
        }
    }

    //删除指定行
    public static void deleteRow(KuduClient client, KuduTable table) throws KuduException {
        KuduSession kuduSession = client.newSession();
        //设置手动刷新
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        Delete delete = table.newDelete();
        delete.getRow().addInt("id", 5);
        kuduSession.apply(delete);
        kuduSession.flush();
        kuduSession.close();
    }

    //kudu 按照范围查询数据
    public static void searchRowWithRange(KuduClient client, KuduTable table) throws KuduException {
        //创建一个数组,并添加相应的表字段
        ArrayList<String> projectColumns = new ArrayList<String>();
        projectColumns.add("id");
        projectColumns.add("name");
        projectColumns.add("score");

        Schema schema = table.getSchema();

        PartialRow partialRow = schema.newPartialRow();
        partialRow.addInt("id", 1);

        PartialRow partialRow1 = schema.newPartialRow();
        partialRow1.addInt("id", 5);

        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns) //指定输出列
                .lowerBound(partialRow) //指定下限(包含)
                .exclusiveUpperBound(partialRow1) //指定上限(不包含)
                .build();
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                System.out.println("range scanner: " + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getDouble(2));
            }
        }
    }

    //按照条件查询数据
    public static void searchWithCondition(KuduClient client, KuduTable table) throws KuduException {
        Schema schema = table.getSchema();

        //创建一个数组,并添加相应的表字段
        ArrayList<String> projectColumns = new ArrayList<String>();
        projectColumns.add("id");
        projectColumns.add("name");
        projectColumns.add("score");

        //创建predicate
        KuduPredicate kuduPredicate = KuduPredicate.newComparisonPredicate(schema.getColumn("id"),
                KuduPredicate.ComparisonOp.EQUAL, 1);
        KuduScanner kuduScanner = client.newScannerBuilder(table)
                .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT) //设置读取快照模式
                .setProjectedColumnNames(projectColumns) //设置要读取的列
                .addPredicate(kuduPredicate) //设置predicate
                .build();

        while (kuduScanner.hasMoreRows()) {
            for (RowResult rowResult : kuduScanner.nextRows()) {
                System.out.println("range scanner: " + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getDouble(2));
            }
        }
    }



//    //添加列 删除列 修改列 都是这个API，调用AlterTableOptions的不同方法来实现
//    private static void addColumn(String columnName, Type columnType) {
//        try {
//            kudu.alterTable(kuduTable.getName(),
//                    new AlterTableOptions().addNullableColumn(columnName, columnType));
//        } catch (KuduException e) {
//            e.printStackTrace();
//        }
//    }
}
