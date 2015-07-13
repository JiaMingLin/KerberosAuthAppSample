package iii.org.tw.data.hadoop.main;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import tw.org.iii.data.hadoop.hbase.SimpleHBase;
import tw.org.iii.data.hadoop.hdfs.SimpleHDFS;

public class Main {

	public static void main(String[] args){

		for(String str : args){
			System.out.println(str);
		}
		if(args.length<=0){
			printAndExit("Please specify the DOMO to run!!!");
		}else{
			if(args[0].equals("hbase")){				
				hbaseDEMO();
			}else if (args[0].equals("hdfs")){
				hdfsDEMO();
			}else{
				printAndExit("The specified DEMO is not existed by now!!!");
			}
		}
		
	}
	
	private static void hdfsDEMO(){
		SimpleHDFS simpleHDFS = SimpleHDFS.getInstance();
		String str;
		while(true){
			System.out.print("Enter commands to run the DEMO: ");
			Scanner in = new Scanner(System.in);
			str = in.nextLine();
			String[] input =  str.split(" ");
			String action = input[0].toLowerCase();
			switch(action){
				case "create_folder":
					simpleHDFS.createFolder(input[1]);
					break;
				case "create_file":
					simpleHDFS.createFile(input[1], input[2]);
					break;
				case "list_object":
					simpleHDFS.list(input[1]);
					break;
				case "read_file":
					simpleHDFS.read(input[1]);
					break;
				case "delete_object":
					simpleHDFS.delete(input[1]);
					break;
				default:
					System.out.println("The input command is invalid");
			}
		}
	}
	
	private static void hbaseDEMO(){
		SimpleHBase simpleHBase;
		try {
			simpleHBase = SimpleHBase.getInstance();
			String tableName = "student";		
			String str = new String();
			while(true){
				System.out.print("Enter commands to run the DEMO: ");
				Scanner in = new Scanner(System.in);
				str = in.nextLine();
				String[] input =  str.split(" ");
				String action = input[0].toLowerCase();
				switch(action){
					case "create":
						simpleHBase.createTable(tableName);
						simpleHBase.addData(tableName);
						break;
					case "list_all":
						simpleHBase.getAllData(tableName);
						break;
					case "get":
						simpleHBase.getData(tableName,input[1]);
						break;
					case "get_by_age":
						simpleHBase.simpleFilter(tableName, Integer.valueOf(input[1]), CompareOp.EQUAL);
						break;
					case "delete":
						simpleHBase.deleteDate(tableName, input[1]);
						break;
					case "drop_table":
						simpleHBase.dropTable(tableName);
						break;
					default:
						System.out.println("Invalid input");
						break;
				}
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void printAndExit(String message){
		System.out.println(message);
		System.exit(0);
	}
}
