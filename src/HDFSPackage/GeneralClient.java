package HDFSPackage;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

import HDFSPackage.RequestResponse.AssignBlockRequest;
import HDFSPackage.RequestResponse.AssignBlockResponse;
import HDFSPackage.RequestResponse.BlockLocationRequest;
import HDFSPackage.RequestResponse.BlockLocationResponse;
import HDFSPackage.RequestResponse.BlockLocations;
import HDFSPackage.RequestResponse.CloseFileRequest;
import HDFSPackage.RequestResponse.CloseFileResponse;
import HDFSPackage.RequestResponse.DataNodeLocation;
import HDFSPackage.RequestResponse.OpenFileRequest;
import HDFSPackage.RequestResponse.OpenFileRespose;
import HDFSPackage.RequestResponse.ReadBlockRequest;
import HDFSPackage.RequestResponse.ReadBlockResponse;

public class GeneralClient {
	/* openFile will return the response of */
	public byte[] open(String fileName, boolean forRead) {
		byte[] response = null;
		int status = -1;
		try {
			Registry myreg = LocateRegistry.getRegistry(
					AllDataStructures.nameNodeIP,
					AllDataStructures.nameNodePort);
			INameNode in = (INameNode) myreg.lookup("NameNode");
			OpenFileRequest openFileRequest = new OpenFileRequest("temp.txt",
					forRead);
			response = in.openFile(openFileRequest.toProto());
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
		}
		if (status == -1) {
			OpenFileRespose openFileResponse = new OpenFileRespose(-1, -1, null);
			response = openFileResponse.toProto();
		}

		return response;
	}

	// close File
	int close(int fileHandle) {
		int status = 1;
		INameNode in = null;
		CloseFileRequest closeFileRequest = new CloseFileRequest(fileHandle);

		Registry myreg;
		try {
			myreg = LocateRegistry.getRegistry(AllDataStructures.nameNodeIP,
					AllDataStructures.nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");

			byte[] closeFileRes;
			closeFileRes = in.closeFile(closeFileRequest.toProto());
			CloseFileResponse closeFileResponse = new CloseFileResponse(
					closeFileRes);
			status = closeFileResponse.status;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			status = -1;
		}

		return status;
	}

	// read file
	public int read(String fileName) {

		int status = 0;

		// Open File for read
		byte[] openResponse;
		openResponse = open(fileName, true);
		OpenFileRespose openFileResponse = new OpenFileRespose(openResponse);

		// check status of openFile
		if (openFileResponse.status == -1) {

			// openFile Unsuccessful
			status = -1;
			return status;
		}

		INameNode in = null;
		IDataNode dataNode = null;
		try {
			Registry myreg = LocateRegistry.getRegistry(
					AllDataStructures.nameNodeIP,
					AllDataStructures.nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			status = -1;
			// e.printStackTrace();
			return status;
		}

		// get locations for list of blocks returned by openFile
		byte[] getBlockLocationResponse;
		BlockLocationRequest blockLocationRequest = new BlockLocationRequest(
				openFileResponse.blockNums);
		try {
			getBlockLocationResponse = in
					.getBlockLocations(blockLocationRequest.toProto());
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			status = -1;
			return status;
		}

		BlockLocationResponse blockLocationResponse = new BlockLocationResponse(
				getBlockLocationResponse);

		// check status of getBlockLocations
		if (blockLocationResponse.status == -1) {
			status = -1;
			return status;
		}

		// repeat for each block number
		for (int i = 0; i < blockLocationResponse.blockLocations.size(); i++) {
			BlockLocations blockLocation = blockLocationResponse.blockLocations
					.get(i);
			int blockNum = blockLocation.blockNumber;
			System.out.println(blockNum);

			// repeat for each dataNodeLocation for that block number
			int j;
			for (j = 0; j < blockLocation.locations.size(); j++) {
				DataNodeLocation dataNodeAddress = blockLocation.locations
						.get(j);

				// check time stamp
				if (dataNodeAddress.tstamp >= System.currentTimeMillis()
						- AllDataStructures.thresholdTime) {

					// get rmi object
					try {
						Registry myreg = LocateRegistry.getRegistry(
								Integer.toString(dataNodeAddress.ip),
								dataNodeAddress.port);
						dataNode = (IDataNode) myreg.lookup("DataNode");

						ReadBlockRequest readBlockRequest = new ReadBlockRequest(
								blockNum);
						byte[] buffer;
						buffer = dataNode.readBlock(readBlockRequest.toProto());
						ReadBlockResponse readBlockResponse = new ReadBlockResponse(
								buffer);

						if (readBlockResponse.status == 1) {
							System.out.println(readBlockResponse.data
									.toString());
							break;
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
			if (j == blockLocation.locations.size()) {
				status = -1;
				return status;
			}
		}

		int status1 = close(openFileResponse.handle);
		return status*status1;
	}

	public int write(String fileName, byte[] data) {
		int status = -1;
		float dataSize = data.length;
		int numOfBlocksReq = (int) Math.ceil(dataSize
				/ AllDataStructures.blockSize);

		byte[] openResponse;
		openResponse = open(fileName, false);
		OpenFileRespose openFileResponse = new OpenFileRespose(openResponse);

		// check status of openFile
		if (openFileResponse.status == -1) {

			// openFile Unsuccessful
			status = -1;
			return status;
		}

		INameNode in = null;
		IDataNode dataNode = null;
		try {
			Registry myreg = LocateRegistry.getRegistry(
					AllDataStructures.nameNodeIP,
					AllDataStructures.nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
			return status;
		}

		if (openFileResponse.status == 1) {
			// if file exist already in write mode
		} else if (openFileResponse.status == 2) {

			// create new file in write mode
			int i;
			int offset = 0;
			for (i = 0; i < numOfBlocksReq; i++) {
				try {
					AssignBlockRequest assignBlockRequest = new AssignBlockRequest(
							openFileResponse.handle);
					byte[] assignBlockRes = in.assignBlock(assignBlockRequest
							.toProto());

					AssignBlockResponse assignBlockResponse = new AssignBlockResponse(
							assignBlockRes);

					// assign block fails
					if (assignBlockResponse.status == -1) {
						status = -1;
						break;
					}

					BlockLocations blockLocations = assignBlockResponse.newBlock;
					int j;
					for (j = 0; j < blockLocations.locations.size(); j++) {
						DataNodeLocation dataNodeAddress = blockLocations.locations
								.get(j);

						// check time stamp
						if (dataNodeAddress.tstamp >= System
								.currentTimeMillis()
								- AllDataStructures.thresholdTime) {

							// get rmi object
							try {
								Registry myreg = LocateRegistry.getRegistry(
										Integer.toString(dataNodeAddress.ip),
										dataNodeAddress.port);
								dataNode = (IDataNode) myreg.lookup("DataNode");
								
								// copy chunk of data into buffer
								byte[] buffer = Arrays.copyOfRange(data,
										offset, offset
												+ AllDataStructures.blockSize);
								dataNode.writeBlock(buffer);
								offset += AllDataStructures.blockSize;
							} catch (Exception e) {

								status = -1;
								e.printStackTrace();
								break; // if data node not available then
										// failure
							}
						} else // if that data node not available
						{
							status = -1;
							break; // if data node not available then failure
						}
					}

				} catch (RemoteException e) {
					status = -1;
					e.printStackTrace();
					break;
				}
			}
		}
		// close file
		
		int status1 = close(openFileResponse.handle);
		return status*status1;
	}

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
