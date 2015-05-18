package idv.jiaming.MapReducePractice;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.ServiceException;

public class HBaseAuth {

	private static final String SAMPLE_PRINCIPLE = "hue/allinone@EXAMPLE.COM";
	private static final String SAMPLE_KEYTAB = "/hue.service.keytab";
	private static final String SAMPLE_PROXY_USER = "it1";
	private static final String SAMPLE_TABLE_NAME = "people";
	 
	public static void main(String[] args) throws InterruptedException,
			IOException {

		try {
			Configuration conf = new Configuration();
			UserGroupInformation.setConfiguration(conf);
			// using the registered principle and keytab to login Kerberos.
			UserGroupInformation.loginUserFromKeytab(SAMPLE_PRINCIPLE, SAMPLE_KEYTAB);

			// a superuser(service name in the SAMPLE_PRINCIPLE) can access hbase on behalf of another user in a secured way.
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(SAMPLE_PROXY_USER, UserGroupInformation.getLoginUser());
			// Impersonate as the SAMPLE_PROXY_USER user.
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() {
					// create HBase configuration according to the file hbase-site.xml in resources folder.
					Configuration conf = HBaseConfiguration.create();
					
					try {
						// check HBase status
						HBaseAdmin.checkHBaseAvailable(conf);						
						System.out.println("User '"+SAMPLE_PROXY_USER+"' Checks HBase status OK!!!");

						HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
						if(hbaseAdmin.isTableAvailable(SAMPLE_TABLE_NAME)){
							hbaseAdmin.disableTable(SAMPLE_TABLE_NAME);
							hbaseAdmin.deleteTable(SAMPLE_TABLE_NAME);
						}
						
						// Create table
						System.out.println("User '"+SAMPLE_PROXY_USER+"' Creates Table: "+SAMPLE_TABLE_NAME);
						HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(SAMPLE_TABLE_NAME));
						tableDescriptor.addFamily(new HColumnDescriptor("personal"));
						tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
						tableDescriptor.addFamily(new HColumnDescriptor("creditcard"));
						hbaseAdmin.createTable(tableDescriptor);
						
						// put some data
						System.out.println("User '"+SAMPLE_PROXY_USER+"' Inserts same data to Table: "+SAMPLE_TABLE_NAME);
						HTable table = new HTable(conf, SAMPLE_TABLE_NAME);
						Put put = new Put(Bytes.toBytes("doe-john-m-12345"));
						put.add(Bytes.toBytes("personal"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
						put.add(Bytes.toBytes("personal"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
						put.add(Bytes.toBytes("personal"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
						put.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("john.m.doe@gmail.com"));
						table.put(put);
						
						// scan table
						Scan scan = new Scan();						
						scan.setFilter(new PageFilter(25));
						ResultScanner scanner = table.getScanner(scan);
						for (Result result : scanner) {
						    System.out.println("User '"+SAMPLE_PROXY_USER+"' Scans Table result: "+result.toString());
						}
						table.flushCommits();
						table.close();
						
						// delete table						
						hbaseAdmin.disableTable(SAMPLE_TABLE_NAME);
						hbaseAdmin.deleteTable(SAMPLE_TABLE_NAME);
						// Delete table success!!!
						System.out.println("User '"+SAMPLE_PROXY_USER+"' Deletes Table '"+SAMPLE_TABLE_NAME+"' Success!!!");
						
						System.out.println("============== List Table Name =======================");
						TableName[] tableNames = hbaseAdmin.listTableNames();
						for(int i=0 ; i< tableNames.length;i++){
							System.out.println(tableNames[i].getNameAsString());
						}
						
					} catch (MasterNotRunningException e) {
						e.printStackTrace();
					} catch (ZooKeeperConnectionException e) {
						e.printStackTrace();
					} catch (ServiceException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

					return null;

				}
			});
		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}

}
