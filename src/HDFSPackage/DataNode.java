package HDFSPackage;

import java.io.*;
import java.util.ArrayList;

import HDFSPackage.RequestResponse.*;

public class DataNode implements IDataNode{

	@Override
	public byte[] readBlock(byte[] input) {
		// TODO Auto-generated method stub
		ReadBlockRequest readBlockRequest = new ReadBlockRequest(input);
		ReadBlockResponse readBlockResponse = new ReadBlockResponse();
		try{
			File file = new File(""+readBlockRequest.blockNumber);
			if(file.exists()){
				FileInputStream fis = new FileInputStream(file);
				byte[] data = new byte[(int) file.length()];
				fis.read(data);
				readBlockResponse.data = data;
				readBlockResponse.status = 1;
				fis.close();
			}
			else{
				readBlockResponse.status = -1;
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
		//readBlockRequest.blockNumber;
		return readBlockResponse.toProto();
	}

	@Override
	public byte[] writeBlock(byte[] input) {
		// TODO Auto-generated method stub
		WriteBlockRequest writeBlockRequest = new WriteBlockRequest(input);
		WriteBlockResponse writeBlockResponse = new WriteBlockResponse();
		File  fileName = new File("" + writeBlockRequest.blockInfo.blockNumber);
		ArrayList<DataNodeLocation> dataNodes = writeBlockRequest.blockInfo.locations;
		byte []data = writeBlockRequest.data;
		
		/*Take data Node information from blockInfo Chaining call*/
		return null;
	}

}
