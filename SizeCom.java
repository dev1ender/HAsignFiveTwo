package FiveTwo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class SizeCom implements WritableComparable<SizeCom>{
	
	

	private int size;
	private String Company;
	private String product;
	
	public int getSize() {
		return size;
	}

	public String getCompany() {
		return Company;
	}
	
	public String getproduct() {
		return product;
	}

	public void set(int size,String company,String product) {
		Company = company;
		this.size = size;
		this.product =product;
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		Company = in.readLine();
		product = in.readLine();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		out.writeChars(Company);
		out.writeChars(product);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return size + "|" + Company+"|"+product;
	}

	@Override
	public int compareTo(SizeCom sizecom) {
			
		int cmp = (size - sizecom.getSize());
		if(cmp !=0){
			return (-1)*cmp;
		}
		
		return 0;
	
	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		if(Company.equals("Samsung") ){
			return 0;
		}
		if(Company.equals("Onida") ){
			return 1;
		}
		if(Company.equals("Zen") ){
			return 2;
		}
		if(Company.equals("Akai") ){
			return 3;
		}
		if(Company.equals("NA") ){
			return 4;
		}
		
		else
		return 5;
		
	}
}


