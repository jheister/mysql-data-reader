package jheister.mysqldatareader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static jheister.mysqldatareader.Experiment.Page.pageAt;
import static jheister.mysqldatareader.Experiment.RowFormat.COMPACT;
import static jheister.mysqldatareader.Experiment.RowFormat.REDUNDANT;

public class Experiment {
    public static void main(String[] args) throws IOException {
        RandomAccessFile file = new RandomAccessFile("/usr/local/var/mysql/tim/database_upgrade.ibd", "r");
        MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());

        int pageCount = (int) (file.length() / (1024 * 16));

        for (int i = 0; i < pageCount; i++) {
            int offset = pageOffset(i);
            Page page = pageAt(buffer, offset);
            System.out.println("Reading page " + i + " at offset " + offset / 1024 + "K");
            System.out.println("\tNumber " + page.getPageNumber());
            System.out.println("\tType " + page.getType());
            System.out.println("\tPrevious " + page.getPreviousPage());
            System.out.println("\tNext " + page.getNextPage());
            if (page instanceof IndexPage) {
                System.out.println("\tLevel " + ((IndexPage) page).getLevel());
                System.out.println("\t# Records " + ((IndexPage) page).getNumberOfRecords());
                System.out.println("\tIndex Id " + ((IndexPage) page).getIndexId());
                System.out.println("\tRow format " + ((IndexPage) page).getRowFormat());
            }
        }
    }

    private static int pageOffset(int pageNumber) {
        return 1024 * 16 * pageNumber;
    }

    //see: https://github.com/jeremycole/innodb_ruby/blob/a9ad06c8aea200ff0a22c28ad6e251232ab10315/lib/innodb/page.rb
    public static class Page {
        protected final ByteBuffer buffer;

        public Page(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public static Page pageAt(ByteBuffer buffer, int offset) {
            buffer.position(offset);
            ByteBuffer pageBuffer = buffer.slice();
            pageBuffer.limit(16 * 1024);

            PageType pageType = PageType.valueOf(pageBuffer.getShort(24));

            switch (pageType) {
                case INDEX:
                    return new IndexPage(pageBuffer);
                default:
                    return new Page(pageBuffer);
            }
        }

        public int getPageNumber() {
            return buffer.getInt(4);
        }

        public PageType getType() {
            return PageType.valueOf(buffer.getShort(24));
        }

        public int getPreviousPage() {
            return buffer.getInt(8);
        }

        public int getNextPage() {
            return buffer.getInt(12);
        }
    }

    public static class IndexPage extends Page {
        public IndexPage(ByteBuffer buffer) {
            super(buffer);
        }

        public short getLevel() {
            return buffer.getShort(64);
        }

        public int getIndexId() {
            return buffer.getInt(66);
        }

        public short getNumberOfRecords() {
            return buffer.getShort(54);
        }

        public RowFormat getRowFormat() {
            return (buffer.getShort(42) & 1<<15) == 0 ? REDUNDANT : COMPACT;
        }

        //todo: expose iterator over records linked list
    }

    public enum RecordType {
        CONVENTIONAL,
        NODE_POINTER,
        INFIMUM,
        SUPERMUM
    }

    public enum RowFormat {
        REDUNDANT,
        COMPACT;
    }

    public enum PageType {
        ALLOCATED(0),
        UNDO_LOG(2),
        INODE(3),
        IBUF_FREE_LIST(4),
        IBUF_BITMAP(5),
        SYS(6),
        TRX_SYS(7),
        FSP_HDR(8),
        XDES(9),
        BLOB(10),
        ZBLOB2(12),
        INDEX(17855);

        private short value;

        PageType(int value) {
            this.value = (short) value;
        }

        public static PageType valueOf(short value) {
            PageType[] values = values();
            for (int i = 0; i < values.length; i++) {
                if (values[i].value == value) {
                    return values[i];
                }
            }
            throw new RuntimeException("Unknown page type " + value);
        }
    }
}
