package mapreduceJobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserInfo implements Writable{

	
	Text userId; 
	Text rating;
	
	
	public UserInfo() {
		userId = new Text();
		rating = new Text();
	}

	public UserInfo(Text _userId, Text _rating) {
		userId = _userId;
		rating = _rating;
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
	}


	@Override
	public void write(DataOutput dataOutput) throws IOException {
		userId.write(dataOutput);
		rating.write(dataOutput);
		
	}
	
	@Override
	public String toString(){
		return userId.toString() + " " +rating.toString();
		
	}

	@Override
	public int hashCode() {
	   
	  return userId.hashCode();
	}

}
