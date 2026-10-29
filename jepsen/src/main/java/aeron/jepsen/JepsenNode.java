/*
 * Copyright 2026 the Aeron Jepsen suite authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package aeron.jepsen;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A member of the Jepsen cluster, in its container: a media driver, an
 * archive, a consensus module and the list-append service, in one JVM.
 *
 * <pre>
 * JepsenNode --id N --hosts IP,IP,IP [--data /data] [--aeron-dir /dev/shm/aeron]
 *     [--heartbeat-timeout-ms 2000] [--heartbeat-interval-ms 100] [--election-timeout-ms 1000]
 *     [--startup-canvass-ms 5000]
 * </pre>
 *
 * Member N's endpoints, on its host: ingress 9002, consensus 9003, log 9004,
 * catch-up 9005, archive control 8010.
 */
public final class JepsenNode
{
    static final String LOCAL_CONTROL = "aeron:ipc?term-length=64k";

    public static void main(final String[] args)
    {
        final Map<String, String> opts = parse(args);
        final int id = Integer.parseInt(opts.get("id"));
        final String[] hosts = opts.get("hosts").split(",");
        final String host = hosts[id];
        final File data = new File(opts.getOrDefault("data", "/data"));
        final String aeronDir = opts.getOrDefault("aeron-dir", "/dev/shm/aeron");
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .termBufferSparseFile(true)
            .errorHandler(Throwable::printStackTrace);

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(data, "archive"))
            .archiveId(id)
            .controlChannel("aeron:udp?endpoint=" + host + ":8010|term-length=64k")
            .localControlChannel(LOCAL_CONTROL)
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .errorHandler(Throwable::printStackTrace);

        final AeronArchive.Context archiveClient = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(LOCAL_CONTROL)
            .controlResponseChannel(LOCAL_CONTROL)
            .aeronDirectoryName(aeronDir);

        final ConsensusModule.Context consensusContext = new ConsensusModule.Context()
            .aeronDirectoryName(aeronDir)
            .clusterMemberId(id)
            .clusterMembers(members(hosts))
            .clusterDir(new File(data, "cluster"))
            .serviceCount(1)
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel("aeron:udp?term-length=1m")
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .archiveContext(archiveClient.clone())
            .leaderHeartbeatTimeoutNs(millis(opts, "heartbeat-timeout-ms", 2_000))
            .leaderHeartbeatIntervalNs(millis(opts, "heartbeat-interval-ms", 100))
            .electionTimeoutNs(millis(opts, "election-timeout-ms", 1_000))
            .startupCanvassTimeoutNs(millis(opts, "startup-canvass-ms", 5_000))
            .terminationHook(barrier::signalAll)
            .errorHandler(Throwable::printStackTrace);

        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .archiveContext(archiveClient.clone())
            .clusterDir(new File(data, "cluster"))
            .clusteredService(new ListAppendService())
            .terminationHook(barrier::signalAll)
            .errorHandler(Throwable::printStackTrace);

        MediaDriver driver = null;
        Archive archive = null;
        ConsensusModule consensusModule = null;
        ClusteredServiceContainer container = null;
        try
        {
            driver = MediaDriver.launch(driverContext);
            archive = Archive.launch(archiveContext);
            consensusModule = ConsensusModule.launch(consensusContext);
            container = ClusteredServiceContainer.launch(serviceContext);
        }
        catch (final RuntimeException ex)
        {
            // Such as an archive refusing a mark file still active after a kill. The driver's threads would keep
            // the JVM up without the rest, and the barrier's shutdown hook would hold up an exit until its close:
            // halt, as a crash would, for the harness to start the member again.
            ex.printStackTrace();
            System.err.flush();
            Runtime.getRuntime().halt(1);
        }
        System.out.println("READY node=" + id);
        System.out.flush();
        barrier.await();
        CloseHelper.quietCloseAll(container, consensusModule, archive, driver);
        System.out.println("EXIT node=" + id);
    }

    /** Java's members string: id, then ingress, consensus, log, catch-up and archive endpoints. */
    static String members(final String[] hosts)
    {
        final StringBuilder members = new StringBuilder();
        for (int i = 0; i < hosts.length; i++)
        {
            final String h = hosts[i];
            members.append(i)
                .append(',').append(h).append(":9002")
                .append(',').append(h).append(":9003")
                .append(',').append(h).append(":9004")
                .append(',').append(h).append(":9005")
                .append(',').append(h).append(":8010")
                .append('|');
        }
        return members.toString();
    }

    private static long millis(final Map<String, String> opts, final String name, final long defaultMs)
    {
        return TimeUnit.MILLISECONDS.toNanos(Long.parseLong(opts.getOrDefault(name, Long.toString(defaultMs))));
    }

    private static Map<String, String> parse(final String[] args)
    {
        final Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2)
        {
            if (!args[i].startsWith("--"))
            {
                throw new IllegalArgumentException("expected an option, got " + args[i]);
            }
            opts.put(args[i].substring(2), args[i + 1]);
        }
        if (!opts.containsKey("id") || !opts.containsKey("hosts"))
        {
            throw new IllegalArgumentException("--id and --hosts are required");
        }
        return opts;
    }
}
