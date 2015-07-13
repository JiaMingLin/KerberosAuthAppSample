package tw.org.iii.data.hadoop.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleHBase {
	private static SimpleHBase singleton = null;
	private static Configuration conf = null;
	private static HBaseAdmin admin = null;

	private SimpleHBase() {
	}

	public static SimpleHBase getInstance() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		if (singleton == null) {
			singleton = new SimpleHBase();
			conf = HBaseConfiguration.create();
			admin = new HBaseAdmin(conf);
		}
		return singleton;
	}

	private Map<String, Integer> studentInfo() {
		Map<String, Integer> infoMap = new HashMap<String, Integer>();

		infoMap.put("robin", 18);
		infoMap.put("tracy", 20);
		infoMap.put("steven", 17);
		infoMap.put("cat", 21);
		infoMap.put("page", 25);
		infoMap.put("jimmy", 23);
		infoMap.put("don", 21);
		infoMap.put("macleny", 24);
		infoMap.put("chapman", 19);
		infoMap.put("eric", 19);
		infoMap.put("clapton", 21);

		return infoMap;
	}

	/**
	 * create a new Table
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,the new Table's name
	 * */
	public void createTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + "is exist ,delete ......");
			}

			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			tableDescriptor.addFamily(new HColumnDescriptor("info"));
			tableDescriptor.addFamily(new HColumnDescriptor("address"));
			admin.createTable(tableDescriptor);
			System.out.println("end create table");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Delete the existing table
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,Table's name
	 * */
	public void dropTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + "delete success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * insert a data
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,Table's name
	 * */
	public void addData(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				HTable table = new HTable(conf, tableName);

				Map<String, Integer> infos = studentInfo();

				for (String name : infos.keySet()) {
					Put put = new Put(Bytes.toBytes(name));
					String ageStr = String.valueOf(infos.get(name));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("age"),
							Bytes.toBytes(ageStr));
					table.put(put);
				}

				System.out.println("add success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/**
	 * Delete a data
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,Table's name
	 * */
	public void deleteDate(String tableName, String rowKey) {
		try {
			if (admin.tableExists(tableName)) {
				HTable table = new HTable(conf, tableName);
				Delete delete = new Delete(Bytes.toBytes(rowKey));
				table.delete(delete);
				System.out.println("delete success!");
			} else {
				System.out.println("Table does not exist!");
			}
		}  catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * get a data
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,Table's name
	 * */
	public void getData(String tableName,
			String rowKey) {
		HTable table;
		try {
			table = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);

			for (Cell cell : result.rawCells()) {
				System.out.println("RowName:"
						+ new String(CellUtil.cloneRow(cell)) + " ");
				System.out.println("Timetamp:" + cell.getTimestamp() + " ");
				System.out.println("column Family:"
						+ new String(CellUtil.cloneFamily(cell)) + " ");
				System.out.println("row Name:"
						+ new String(CellUtil.cloneQualifier(cell)) + " ");
				System.out.println("value:"
						+ new String(CellUtil.cloneValue(cell)) + " ");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * insert all data
	 * 
	 * @param configuration
	 *            Configuration
	 * @param tableName
	 *            String,Table's name
	 * */
	public void getAllData(String tableName) {
		HTable table;
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				for (Cell cell : result.rawCells()) {
					System.out.println("RowName:"
							+ new String(CellUtil.cloneRow(cell)) + " ");
					System.out.println("Timetamp:" + cell.getTimestamp() + " ");
					System.out.println("column Family:"
							+ new String(CellUtil.cloneFamily(cell)) + " ");
					System.out.println("row Name:"
							+ new String(CellUtil.cloneQualifier(cell)) + " ");
					System.out.println("value:"
							+ new String(CellUtil.cloneValue(cell)) + " ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void simpleFilter(String table,
			int age, CompareFilter.CompareOp operator) {
		Filter colFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
				Bytes.toBytes("age"), CompareFilter.CompareOp.EQUAL,
				Bytes.toBytes(String.valueOf(age)));
		try {
			HTable hTable = new HTable(conf, table);
			Scan scan = new Scan();
			scan.setFilter(colFilter);

			ResultScanner results = hTable.getScanner(scan);
			for (Result result : results) {
				for (Cell cell : result.rawCells()) {
					System.out.println("RowName:"
							+ new String(CellUtil.cloneRow(cell)) + " ");
					System.out.println("Timetamp:" + cell.getTimestamp() + " ");
					System.out.println("column Family:"
							+ new String(CellUtil.cloneFamily(cell)) + " ");
					System.out.println("Qualifier Name:"
							+ new String(CellUtil.cloneQualifier(cell)) + " ");
					System.out.println("value:"
							+ new String(CellUtil.cloneValue(cell)) + " ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
