package idv.jiaming.MapReducePractice;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSAuth {

	private static final String SAMPLE_PRINCIPLE = "hue/allinone@EXAMPLE.COM";
	private static final String SAMPLE_KEYTAB = "/hue.service.keytab";
	private static final String SAMPLE_PROXY_USER = "it1";
	private static final String SAMPLE_FOLDER_NAME = "people";
	
	public static void main(String[] args) {
		
		// create HDFS configuration according to the files core-site.xml and hdfs-site.xml in resources folder.
		Configuration conf = new Configuration();
		
		// configure the client to authenticate with Kerberos.
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		try {
			
			// using the registered principle and keytab to login Kerberos.
			UserGroupInformation.loginUserFromKeytab(SAMPLE_PRINCIPLE,SAMPLE_KEYTAB);
			
			// a superuser(service name in the SAMPLE_PRINCIPLE) can access hdfs on behalf of another user in a secured way.
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(SAMPLE_PROXY_USER, UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws IOException {
					
					Configuration conf = new Configuration();
					// initiate an instance of file system
					// put the files core-site.xml and hdfs-site,xml in the resource folder so that the client knows the HDFS location.
					FileSystem fs = FileSystem.get(conf);
					
					// create a folder.
					Path path = new Path("/tmp/"+SAMPLE_FOLDER_NAME);			
					fs.mkdirs(path);
					System.out.println("User '"+SAMPLE_PROXY_USER+"' Creates folder '"+path.toString()+"' sucess! ");
					
					// list a folder
					Path parentPath = new Path("/tmp");
					FileStatus[] list = fs.listStatus(parentPath);
					for(int i=0; i< list.length ;i++){
						System.out.println("User '"+SAMPLE_PROXY_USER+"' Lists Status: "+list[i].getPath());
					}
					
					// delete a folder
					fs.deleteOnExit(path);
					System.out.println("User '"+SAMPLE_PROXY_USER+"' Deletes folder "+path.toString()+" sucess! ");
					return null;
					
				}
			});
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
