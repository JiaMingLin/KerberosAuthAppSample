package tw.org.iii.data.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SimpleHDFS {

	private static Configuration conf;
	private static FileSystem fs = null;
	private static SimpleHDFS singlton = null;

	private SimpleHDFS() {
	}

	public static SimpleHDFS getInstance() {
		if (singlton == null) {
			singlton = new SimpleHDFS();
			conf = new Configuration();
		}

		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return singlton;
	}

	public void createFolder(String pathStr) {
		Path path = new Path(pathStr);
		System.out.println("Create folders recursively: "+ pathStr);
		try {
			// Make the given file and all non-existent parents into
			// directories.
			fs.mkdirs(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createFile(String pathStr, String content) {

		byte[] byt = content.getBytes();
		Path path = new Path(pathStr);
		try {
			FSDataOutputStream fsOutStream = fs.create(path);
			fsOutStream.write(byt);
			fsOutStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void list(String pathStr) {
		Path path = new Path(pathStr);
		System.out.println("List the objects under the path: "+ pathStr);
		try {
			FileStatus[] list = fs.listStatus(path);
			for(int i=0; i< list.length ;i++){
				System.out.println("List Status: "+list[i].getPath());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void read(String pathStr) {
		Path path = new Path(pathStr);
		BufferedReader bfr;
		
		System.out.println("Reading the file: "+ pathStr);
		try {
			bfr = new BufferedReader(new InputStreamReader(fs.open(path)));
			String str = null;
			while ((str = bfr.readLine()) != null) {
				System.out.println(str);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void delete(String pathStr) {
		Path path = new Path(pathStr);
		try {
			fs.deleteOnExit(path);
			System.out.println("File/Folder delete sucess");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
