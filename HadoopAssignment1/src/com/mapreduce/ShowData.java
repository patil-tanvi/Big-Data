package com.mapreduce;

import java.io.File;
import java.util.Scanner;

/**
 * Class that shows the contents of the file specified in filename
 * @author tanvi
 *
 */
public class ShowData {
	static String filename = "/Users/tanvi/Box Sync/Projects/Big Data/dataset/users.dat";
	
	public static void main(String args[]){
		try{
		Scanner in = new Scanner(new File(filename));
	
		while(in.hasNext()){
			System.out.println(in.next());
		}
		in.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}
