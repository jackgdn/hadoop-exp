public class SortReducer extends Reducer<NullWritable, IntWritable, IntWritable, IntWritable> {
    private List<Integer> numbers = new ArrayList<>();

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {
            numbers.add(value.get());
        }
        Collections.sort(numbers);
        int index = 1;
        for (int num : numbers) {
            context.write(new IntWritable(index++), new IntWritable(num));
        }
    }
}
