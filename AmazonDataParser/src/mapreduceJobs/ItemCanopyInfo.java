package mapreduceJobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ItemCanopyInfo  implements Writable{

	Text itemId;
	Text userId; 
	Text rating;
	Text canopyId;
	
	public ItemCanopyInfo() {
		userId = new Text();
		rating = new Text();
		itemId = new Text();
		canopyId = new Text();
	}

	public ItemCanopyInfo( Text _itemId, Text _userId, Text _rating, Text _canopyId) {
		itemId = _itemId;
		userId = _userId;
		rating = _rating;
		canopyId = _canopyId; 
	}
	

	public Text getItemId() {
		return itemId;
	}

	public void setItemId(Text itemId) {
		this.itemId = itemId;
	}

	public Text getCanopyId() {
		return canopyId;
	}

	public void setCanopyId(Text canopyId) {
		this.canopyId = canopyId;
	}

	public Text getUserId() {
		return userId;
	}


	public void setUserId(Text userId) {
		this.userId = userId;
	}


	public Text getRating() {
		return rating;
	}


	public void setRating(Text rating) {
		this.rating = rating;
	}


	@Override
	public void readFields(DataInput dataInput) throws IOException {
		userId.readFields(dataInput);
		rating.readFields(dataInput);
		itemId.readFields(dataInput);
		canopyId.readFields(dataInput);
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		userId.write(dataOutput);
		rating.write(dataOutput);
		itemId.write(dataOutput);
		canopyId.write(dataOutput);
		
	}
	
	@Override
	public String toString(){
		return userId.toString() + " " +rating.toString() +" "+canopyId;
		
	}

	@Override
	public int hashCode() {
	   
	  return itemId.hashCode();
	}

}

