package HDFSPackage;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import HDFSPackage.RequestResponse.OpenFileRequest;

public class GeneralClient {
	/* openFile will return the response of */
	//int open(fileName,forRead)
	//void close(fileHandle)
	//getblock()
	//assignBlock()
	//
	public static void main(String[] args) {
		try {
			Registry myreg = LocateRegistry.getRegistry("127.0.0.1", 1099);
			INameNode in = (INameNode) myreg.lookup("NameNode");
			OpenFileRequest openFileRequest = new OpenFileRequest("temp.txt",
					false);
			in.openFile(openFileRequest.toProto());
			in.test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
