package HDFSPackage;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

import HDFSPackage.RequestResponse.*;

class PerodicService extends TimerTask {
	INameNode in;

	public PerodicService(INameNode in) {
		super();
		this.in = in;
	}

	byte[] generateBlockReport() {
		File file = new File("metadata");
		if (file.exists()) {
			FileInputStream fis;
			try {
				fis = new FileInputStream(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fis));
				String line = reader.readLine();
				fis.close();

				String[] blockNums = line.split(",");
				ArrayList<Integer> blockList = new ArrayList<Integer>();
				for (String num : blockNums) {
					blockList.add(Integer.parseInt(num));
				}
				BlockLocationRequest blockLocationList = new BlockLocationRequest(
						blockList);
				return blockLocationList.toProto();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return null;
	}

	byte[] generateDataNodeAddress() {
		HeartBeatRequest heartBeatRequest = new HeartBeatRequest(
				DataNode.dataNodeID);
		return heartBeatRequest.toProto();
	}

	@Override
	public void run() {
		try {
			in.heartBeat(generateDataNodeAddress());
			in.blockReport(generateBlockReport());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

public class DataNode implements IDataNode {

	public static int dataNodeID = 10;
	public static String nameNodeIP = "127.0.0.1";
	public static int heartBeatRate = 5000;

	@Override
	public byte[] readBlock(byte[] input) {
		// TODO Auto-generated method stub
		ReadBlockRequest readBlockRequest = new ReadBlockRequest(input);
		ReadBlockResponse readBlockResponse = new ReadBlockResponse();
		try {
			File file = new File("" + readBlockRequest.blockNumber);
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);
				byte[] data = new byte[(int) file.length()];
				fis.read(data);
				readBlockResponse.data = data;
				readBlockResponse.status = 1;
				fis.close();
			} else {
				readBlockResponse.status = -1;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// readBlockRequest.blockNumber;
		return readBlockResponse.toProto();
	}

	@Override
	public byte[] writeBlock(byte[] input) {
		// TODO Auto-generated method stub
		// status successful = 1 unsuccessful = -1
		int status = 1;
		WriteBlockRequest writeBlockRequest = new WriteBlockRequest(input);
		WriteBlockResponse writeBlockResponse = new WriteBlockResponse();

		// write into file
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(""
					+ writeBlockRequest.blockInfo.blockNumber));
			bw.append(writeBlockRequest.data.toString());
			bw.flush();
			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			status = -1;
			e.printStackTrace();
		}

		// make entry of block number in datanode
		try {
			String meta = "metadata";
			File fis = new File(meta);
			FileWriter fw = null;
			boolean flag = true;
			if (!fis.exists()) {
				flag = false;
				fw = new FileWriter("metadata", false);
			} else {
				fw = new FileWriter("metadata", true);
			}

			BufferedWriter bw = new BufferedWriter(fw);
			if (flag) {
				String data = "," + writeBlockRequest.blockInfo.blockNumber;
				bw.write(data);
			} else {
				String data = "" + writeBlockRequest.blockInfo.blockNumber;
				bw.append(data);
			}
			//
			bw.flush();
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		writeBlockResponse.status = status;
		return writeBlockResponse.toProto();
	}

	public static void main(String[] args) {
		Registry myreg;
		INameNode in = null;
		try {
			myreg = LocateRegistry.getRegistry(nameNodeIP, 1099);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		TimerTask perodicService = new PerodicService(in);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(perodicService, 0, heartBeatRate);
	}
}
