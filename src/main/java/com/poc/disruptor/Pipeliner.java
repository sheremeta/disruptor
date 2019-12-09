package com.poc.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Pipeliner {

    public static void main(String[] args) {

        Disruptor<DbEvent> disruptor = new Disruptor<>(DbEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        disruptor.handleEventsWith(
                new PrepareChangeData(0, 3),
                new PrepareChangeData(1, 3),
                new PrepareChangeData(2, 3)
        ).then(new AddToKafka());

        RingBuffer<DbEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 1000; i++) {
            long next = ringBuffer.next();

            try {
                DbEvent dbEvent = ringBuffer.get(next);

                dbEvent.setPayload("id=" + i + ", text=" + RandomStringUtils.randomAlphabetic(8));

                System.out.println("Create: " + dbEvent);
            } finally {
                ringBuffer.publish(next);
            }
        }

        disruptor.shutdown();
    }

    private static class PrepareChangeData implements EventHandler<DbEvent> {

        private final int ordinal;
        private final int totalHandlers;

        PrepareChangeData(int ordinal, int totalHandlers) {
            this.ordinal = ordinal;
            this.totalHandlers = totalHandlers;
        }

        public void onEvent(DbEvent event, long sequence, boolean endOfBatch) {
            if (sequence % totalHandlers == ordinal) {
                event.setChangeData(new ChangeData(event.payload));

                System.out.println("Parse: " + event);
            }
        }
    }

    private static class AddToKafka implements EventHandler<DbEvent> {

        public void onEvent(DbEvent event, long sequence, boolean endOfBatch) {
            System.out.println("Send to kafka: " + event.changeData);
        }
    }

    private static class DbEvent {

        private static final EventFactory<DbEvent> FACTORY = DbEvent::new;

        private String payload;
        private ChangeData changeData;

        void setPayload(String payload) {
            this.payload = payload;
        }

        void setChangeData(ChangeData changeData) {
            this.changeData = changeData;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("payload", payload)
                    .append("changeData", changeData)
                    .toString();
        }

    }
}
