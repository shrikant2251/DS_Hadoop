package HDFSPackage;

	import java.util.ArrayList;
	import java.util.HashMap;

	import HDFSPackage.Hdfs.*;

	public class AllDataStructures {
		public static HashMap<String,ArrayList<Integer> > fileNameToBlockNum=new HashMap<String,ArrayList<Integer>>();
		public static HashMap<Integer, String> fileHandleToFileName= new HashMap<Integer, String>();
		public static HashMap<Integer, ArrayList<RequestResponse.DataNodeLocation>> blocNumToDataNodeLoc=new HashMap<Integer,ArrayList<RequestResponse.DataNodeLocation>>();
		public static HashMap<Integer,RequestResponse.DataNodeLocation> idToDataNode = new HashMap<Integer, RequestResponse.DataNodeLocation>();
		public static int fileHandel = 0;
		public static int blockNumber = 0;
		public static int replicationFactor=2;
		public static int thresholdTime=1000;
	}


