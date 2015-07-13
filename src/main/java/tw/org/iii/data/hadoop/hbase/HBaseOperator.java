package tw.org.iii.data.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.google.protobuf.ServiceException;

public class HBaseOperator {

	static Logger LOG = Logger.getLogger(HBaseOperator.class);
	static Configuration conf;
	
	private static HBaseOperator hbaseOperator = null;
	private static HBaseAdmin hbaseAdmin = null;
	private HBaseOperator(){}
		
	/**
	 * Initialize the HBase configurations.
	 * @return
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws ServiceException
	 * @throws IOException
	 */
	public static HBaseOperator getHBaseOperator() throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException{
		
		if(hbaseOperator == null){
			hbaseOperator = new HBaseOperator();
			
			// Test connection
			conf = HBaseConfiguration.create();
			HBaseAdmin.checkHBaseAvailable(conf);
			
			hbaseAdmin = new HBaseAdmin(conf);
		}		
		return hbaseOperator;
	}
	
	public boolean createTable(String table, String... colFamily ) throws IOException{
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table));
		for(String colFamilyName: colFamily){
			tableDescriptor.addFamily(new HColumnDescriptor(colFamilyName));
		}
		hbaseAdmin.createTable(tableDescriptor);
		return true;
	}
	
	public TableName[] listTable(){
		TableName[] tableNames = null;
		try {
			tableNames = hbaseAdmin.listTableNames();
		} catch (IOException e) {

		}
		return tableNames;
	}
	
	public boolean deleteTable(String table){
		
		try {
			if(hbaseAdmin.isTableAvailable(table)){
				hbaseAdmin.disableTable(table);
				hbaseAdmin.deleteTable(table);
				return true;
			}else{
				LOG.info("Delete table "+table+" fail: The table is not available.");				
			}
		} catch (IOException e) {
			LOG.error("Delete table "+table+" fail: "+e.getMessage());
		}
		return false;
	}
	
	public void put(String table, JSONObject data){
		try {
			HTable hTable = new HTable(conf, table);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public List<JSONObject> basicFiltering(String table, JSONObject query){
		List<Filter> colFilters = new ArrayList<Filter>();
		Set<String> colFamilies = query.keySet();
		
		// Read the column families
		for(String colFamily: colFamilies){
			// Read the qualifier
			JSONObject columns = query.getJSONObject(colFamily);
			Set<String> qualifiers = columns.keySet();
			for(String qualifier: qualifiers){
				String value = columns.getString(qualifier);
				// Set Filter
				Filter colFilter = new SingleColumnValueFilter(
						Bytes.toBytes(colFamily), 
						Bytes.toBytes(qualifier),
						CompareFilter.CompareOp.EQUAL,
						Bytes.toBytes(value));
				colFilters.add(colFilter);
			}
			
		}
		FilterList filterList = new FilterList(colFilters); 
		try {
			HTable hTable = new HTable(conf, table);
			Scan scan = new Scan();	
			scan.setFilter(filterList);
			// Parse the result to JSON Object.
			ResultScanner scanner = hTable.getScanner(scan);			
			for (Result result : scanner) {
				JSONObject oneRow = new JSONObject();
			    // extract row key
				String oneRowKey = new String(result.getRow());
				
				// extract column family 
				for(Cell cell : result.rawCells()){
					System.out.println("Column Family: "+ new String(cell.getFamilyArray())
					+ ", value: "+ new String(cell.getValueArray()));
				}
				
			}			
			hTable.flushCommits();
			hTable.close();
		} catch (IOException e) {

		}
		
		return null;
	} 
	
	public void update(String table, JSONObject query, JSONObject data){
		
	}
	
	public void delete(String table, JSONObject query){
		
	}
}
