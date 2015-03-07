package HDFSPackage;
import java.util.ArrayList;
import java.util.Arrays;

import HDFSPackage.RequestResponse.BlockLocations;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public interface RequestResponse{ 
public class BlockReportRequest {
	public DataNodeLocation location;
	public ArrayList<Integer> blockNumbers;
	public BlockReportRequest(DataNodeLocation _location, ArrayList<Integer> _blockNumbers){
		location = _location;
		blockNumbers = _blockNumbers;
	}
	
	public BlockReportRequest(byte[] input) {
		HDFS.BlockReportRequest builder = null;
		try {
			builder= HDFS.BlockReportRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		blockNumbers = new ArrayList<Integer>();
		for(int i: builder.getBlockNumbersList())
			blockNumbers.add(i);
		location = new DataNodeLocation(builder.getLocation());
	}

	public byte[] toProto() {
		HDFS.BlockReportRequest.Builder builder = HDFS.BlockReportRequest.newBuilder();
		builder.setLocation(location.toProtoObject());
		for(int i : blockNumbers)
			builder.addBlockNumbers(i);	
		return builder.build().toByteArray();
	}
}

public class BlockReportResponse {
	public ArrayList<Integer> status;

	public BlockReportResponse(byte[] input) {
		HDFS.BlockReportResponse builder = null;
		try {
			builder= HDFS.BlockReportResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = new ArrayList<Integer>();
		for(int i :builder.getStatusList())
			status.add(i);
	}

	public BlockReportResponse(ArrayList<Integer> a) {
		status = a;
	}

	public BlockReportResponse() {
		// TODO Auto-generated constructor stub
	}

	public byte[] toProto() {
		HDFS.BlockReportResponse.Builder builder = HDFS.BlockReportResponse.newBuilder();
		for(int i : status)
			builder.addStatus(i);	
		
		return builder.build().toByteArray();
	}
}

public class DataNodeLocation {
	public int ip;
	public int port;

	public DataNodeLocation(byte[] input) {
		try {
			 HDFS.DataNodeLocation location = HDFS.DataNodeLocation.parseFrom(input);
			 ip = location.getIp();
			 port = location.getPort();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HDFS.DataNodeLocation.Builder toProtoObject() {
		HDFS.DataNodeLocation.Builder locationBuilder = HDFS.DataNodeLocation.newBuilder();
		locationBuilder.setIp(ip);
		locationBuilder.setPort(port);
		return locationBuilder;
	}

	DataNodeLocation(HDFS.DataNodeLocation location) {
		ip = location.getIp();
		port = location.getPort();
	}

	public DataNodeLocation(int _ip, int _port) {
		ip=_ip;
		port = _port;
	}

	public byte[] toProto() {
		HDFS.DataNodeLocation.Builder builder = HDFS.DataNodeLocation.newBuilder();
		builder.setIp(ip);	
		builder.setPort(port);			
		return builder.build().toByteArray();
	}
}

public class HeartBeatRequest {
	public int id;

	public HeartBeatRequest(byte[] input) {
		HDFS.HeartBeatRequest builder = null;
		try {
			builder= HDFS.HeartBeatRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		id =  builder.getId();
	}

	public HeartBeatRequest(int i) {
		id = i;
	}

	public byte[] toProto() {
		HDFS.HeartBeatRequest.Builder builder = HDFS.HeartBeatRequest.newBuilder();
		builder.setId(id);	
		return builder.build().toByteArray();
	}
}

public class HeartBeatResponse {
	public int status;

	public HeartBeatResponse(int _status){
		status = _status;
	}
	public HeartBeatResponse(byte[] input) {
		HDFS.HeartBeatResponse builder = null;
		try {
			builder= HDFS.HeartBeatResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status =  builder.getStatus();
	}

	public HeartBeatResponse() {
		// TODO Auto-generated constructor stub
	}
	public byte[] toProto() {
		HDFS.HeartBeatResponse.Builder builder = HDFS.HeartBeatResponse.newBuilder();
		builder.setStatus(status);	
		return builder.build().toByteArray();
	}
}

public class WriteBlockRequest {
	public BlockLocations blockInfo;
	public byte data[];
	
	public WriteBlockRequest(byte[] input) {
		HDFS.WriteBlockRequest builder = null;
		try {
			builder= HDFS.WriteBlockRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		blockInfo = new BlockLocations(builder.getBlockInfo());
		int indx = 0;
		data = new byte[builder.getDataList().get(0).size()];
		for(ByteString bs : builder.getDataList()){
			byte[] tmp = bs.toByteArray();
			for(byte b : tmp){
				data[indx++] = b;
			}
		}
	}

	public WriteBlockRequest(BlockLocations _blockLocations,byte _data[]) {
		blockInfo =_blockLocations;
		data = _data;
	}

	public byte[] toProto() {
		HDFS.WriteBlockRequest.Builder builder = HDFS.WriteBlockRequest.newBuilder();
		builder.setBlockInfo(blockInfo.toProtoObject());
		builder.addData(ByteString.copyFrom(data));
		return builder.build().toByteArray();
	}
}

public class BlockLocations {
	public int blockNumber;
	public ArrayList<DataNodeLocation> locations;

	public BlockLocations(int _bn, ArrayList<DataNodeLocation> _l){
		blockNumber = _bn;
		locations = _l;
	}
	public BlockLocations(byte[] input) {
		HDFS.BlockLocations builder = null;
		try {
			builder= HDFS.BlockLocations.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		blockNumber = builder.getBlockNumber();
		locations = new ArrayList<RequestResponse.DataNodeLocation>();
		for(HDFS.DataNodeLocation bl : builder.getLocationsList()){
			locations.add(new DataNodeLocation(bl));
		}
	}

	BlockLocations(HDFS.BlockLocations builder){
		blockNumber = builder.getBlockNumber();
		locations = new ArrayList<RequestResponse.DataNodeLocation>();
		for(HDFS.DataNodeLocation bl : builder.getLocationsList()){
			locations.add(new DataNodeLocation(bl));
		}
	}
	
	public BlockLocations() {
		// TODO Auto-generated constructor stub
	}
	public HDFS.BlockLocations.Builder toProtoObject() {
		HDFS.BlockLocations.Builder builder = HDFS.BlockLocations.newBuilder();
		builder.setBlockNumber(blockNumber);
		for(DataNodeLocation location : locations){
			builder.addLocations(location.toProtoObject());
		}
		return builder;
	}

	public byte[] toProto() {
		HDFS.BlockLocations.Builder builder = HDFS.BlockLocations.newBuilder();
		builder.setBlockNumber(blockNumber);
		for(DataNodeLocation location : locations){
			builder.addLocations(location.toProtoObject());
		}
		return builder.build().toByteArray();
	}
}

public class WriteBlockResponse {
	public int status;

	public WriteBlockResponse(int _s){
		status = _s;
	}
	
	public WriteBlockResponse(byte[] input) {
		HDFS.WriteBlockResponse builder = null;
		try {
			builder= HDFS.WriteBlockResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
	}

	public byte[] toProto() {
		HDFS.WriteBlockResponse.Builder builder = HDFS.WriteBlockResponse.newBuilder();
		builder.setStatus(status);	
		return builder.build().toByteArray();
	}
}

public class CloseFileRequest {
	public int handle;

	public CloseFileRequest(int _h){
		handle = _h;
	}
	public CloseFileRequest(byte[] input) {
		HDFS.CloseFileRequest builder = null;
		try {
			builder= HDFS.CloseFileRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		handle = builder.getHandle();
	}

	public byte[] toProto() {
		HDFS.CloseFileRequest.Builder builder = HDFS.CloseFileRequest.newBuilder();
		builder.setHandle(handle);	
		return builder.build().toByteArray();
	}
}

public class CloseFileResponse {
	public int status;
	
	public CloseFileResponse(int _s){
		status = _s;
	}
	public CloseFileResponse(byte[] input) {
		HDFS.CloseFileResponse builder = null;
		try {
			builder= HDFS.CloseFileResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
	}

	public CloseFileResponse() {
		// TODO Auto-generated constructor stub
	}
	public byte[] toProto() {
		HDFS.CloseFileResponse.Builder builder = HDFS.CloseFileResponse.newBuilder();
		builder.setStatus(status);	
		return builder.build().toByteArray();
	}
}

public class BlockLocationRequest {
	ArrayList<Integer> blockNums;

	BlockLocationRequest(byte[] input) {
		HDFS.BlockReportRequest builder = null;
		try {
			builder= HDFS.BlockReportRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		blockNums = (ArrayList<Integer>) builder.getBlockNumbersList();
	}

	public byte[] toProto() {
		HDFS.BlockLocationRequest.Builder builder = HDFS.BlockLocationRequest.newBuilder();
		for (int i:blockNums)
			builder.addBlockNums(i);	
		return builder.build().toByteArray();
	}
}

public class BlockLocationResponse {
	public int status;
	public ArrayList<BlockLocations> blockLocations;

	public BlockLocationResponse(int _s, ArrayList<BlockLocations> _b){
		blockLocations = _b;
		status = _s;
	}
	public BlockLocationResponse(byte[] input) {
		HDFS.BlockLocationResponse builder = null;
		try {
			builder= HDFS.BlockLocationResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
		blockLocations = new ArrayList<RequestResponse.BlockLocations>();
		for(HDFS.BlockLocations b: builder.getBlockLocationsList()){
			blockLocations.add(new BlockLocations(b));
		}
	}

	public BlockLocationResponse() {
		// TODO Auto-generated constructor stub
	}
	public byte[] toProto() {
		HDFS.BlockLocationResponse.Builder builder = HDFS.BlockLocationResponse.newBuilder();
		builder.setStatus(status);
		for(BlockLocations block_Locations : blockLocations){
			builder.addBlockLocations(block_Locations.toProtoObject());
		}
		return builder.build().toByteArray();
	}
}

public class AssignBlockRequest {
	public int handle;

	public AssignBlockRequest(int _h){
		handle = _h;
	}
	
	public AssignBlockRequest(byte[] input) {
		HDFS.AssignBlockRequest builder = null;
		try {
			builder= HDFS.AssignBlockRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		handle = builder.getHandle();
	}

	public byte[] toProto() {
		HDFS.AssignBlockRequest.Builder builder = HDFS.AssignBlockRequest.newBuilder();
		builder.setHandle(handle);	
		return builder.build().toByteArray();
	}
}

public class AssignBlockResponse {
	public int status;
	public BlockLocations newBlock;

	public AssignBlockResponse(byte[] input) {
		HDFS.AssignBlockResponse builder = null;
		try {
			builder= HDFS.AssignBlockResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
		newBlock = new BlockLocations(builder.getNewBlock());
	}

	public AssignBlockResponse(int _s, BlockLocations _n) {
		newBlock = _n;
		status = _s;
	}

	public AssignBlockResponse() {
		// TODO Auto-generated constructor stub
	}

	public byte[] toProto() {
		HDFS.AssignBlockResponse.Builder builder = HDFS.AssignBlockResponse.newBuilder();
		builder.setStatus(status);
		builder.setNewBlock(newBlock.toProtoObject());
		return builder.build().toByteArray();
	}
}

public class ListFilesRequest {
	public String dirName;

	public ListFilesRequest(String _d){
		dirName = _d;
	}
	
	public ListFilesRequest(byte[] input) {
		HDFS.ListFilesRequest builder = null;
		try {
			builder= HDFS.ListFilesRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		dirName = builder.getDirName();
	}

	public byte[] toProto() {
		HDFS.ListFilesRequest.Builder builder = HDFS.ListFilesRequest.newBuilder();
		builder.setDirName(dirName);	
		return builder.build().toByteArray();
	}
}

public class ListFilesResponse {
	public int status;
	public ArrayList<String> fileNames;

	public ListFilesResponse(int _s, ArrayList<String> _f){
		fileNames = _f;
		status = _s;
	}
	
	public ListFilesResponse(byte[] input) {
		HDFS.ListFilesResponse builder = null;
		try {
			builder= HDFS.ListFilesResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
		fileNames = new ArrayList<String>();
		for(String s : builder.getFileNamesList())
			fileNames.add(s);
	}

	public ListFilesResponse() {
		// TODO Auto-generated constructor stub
	}

	public byte[] toProto() {
		HDFS.ListFilesResponse.Builder builder = HDFS.ListFilesResponse.newBuilder();
		builder.setStatus(status);	
		for (String fileName : fileNames)
			builder.addFileNames(fileName);
		return builder.build().toByteArray();
	}
}

public class OpenFileRequest {
	public String fileName;
	public boolean forRead;

	public OpenFileRequest(String _f,boolean _forRead){
		fileName = _f;
		forRead = _forRead;
	}
	
	public OpenFileRequest(byte[] input) {
		HDFS.OpenFileRequest builder = null;
		try {
			builder= HDFS.OpenFileRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		fileName = builder.getFileName();
		forRead = builder.getForRead();
	}

	public byte[] toProto() {
		HDFS.OpenFileRequest.Builder builder = HDFS.OpenFileRequest.newBuilder();
		builder.setFileName(fileName);
		builder.setForRead(forRead);
		return builder.build().toByteArray();
	}
}

public class OpenFileResponse {
	public int status;
	public int handle;
	public ArrayList<Integer> blockNums;

	public OpenFileResponse(int _s,int _h, ArrayList<Integer> _b){
		blockNums = _b;
		status = _s;
		handle = _h;
	}
	public OpenFileResponse(byte[] input) {
		HDFS.OpenFileResponse builder = null;
		try {
			builder= HDFS.OpenFileResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
		handle = builder.getHandle();
		blockNums = new ArrayList<Integer>();
		for(int i: builder.getBlockNumsList())
			blockNums.add(i);
	}

	public OpenFileResponse() {
		// TODO Auto-generated constructor stub
	}
	public byte[] toProto() {
		HDFS.OpenFileResponse.Builder builder = HDFS.OpenFileResponse.newBuilder();
		builder.setStatus(status);	
		builder.setHandle(handle);
		for(int blockNum:blockNums)
			builder.addBlockNums(blockNum);
		return builder.build().toByteArray();
	}
}

public class ReadBlockRequest {
	public int blockNumber;

	public ReadBlockRequest(int _b){
		blockNumber = _b;
	}
	
	public ReadBlockRequest(byte[] input) {
		HDFS.ReadBlockRequest builder = null;
		try {
			builder= HDFS.ReadBlockRequest.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		blockNumber = builder.getBlockNumber();
	}

	public byte[] toProto() {
		HDFS.ReadBlockRequest.Builder builder = HDFS.ReadBlockRequest.newBuilder();
		builder.setBlockNumber(blockNumber);
		return builder.build().toByteArray();
	}
}

public class ReadBlockResponse {
	public int status;
	public byte data[];

	public ReadBlockResponse(int _s, byte[] _d){
		status = _s;
		data = _d;
	}
	public ReadBlockResponse(byte[] input) {
		HDFS.ReadBlockResponse builder = null;
		try {
			builder= HDFS.ReadBlockResponse.parseFrom(input);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		status = builder.getStatus();
		int indx = 0;
		data = new byte[builder.getDataList().get(0).size()];
		for(ByteString bs : builder.getDataList()){
			byte[] tmp = bs.toByteArray();
			for(byte b : tmp){
				data[indx++] = b;
			}
		}
	}

	public byte[] toProto() {
		HDFS.ReadBlockResponse.Builder builder = HDFS.ReadBlockResponse.newBuilder();
		builder.setStatus(status);
		builder.addData(ByteString.copyFrom(data));
		return builder.build().toByteArray();
	}
}
}
