using System;
using System.Collections.Generic;
using System.Linq;
using ProtoBuf;
using Vintagestory.API.Client;
using Vintagestory.API.Common;
using Vintagestory.API.Config;
using Vintagestory.API.MathTools;
using Vintagestory.API.Server;
using Vintagestory.API.Util;

namespace EarthquakeMod
{
    public class EarthquakeConfig
    {
        public bool EnableScheduledEvents = false;
        public int EventsPerYear = 3;
        public int EventNearPlayerRadius = 600;
        public int QuakesPerEventMin = 1;
        public int QuakesPerEventMax = 5;
        public int MagnitudeMin = 1;
        public int MagnitudeMax = 9;
        public int GoodiesPerEventMin = 30;
        public int StonesPerEventMin = 300;
        public int DepthClampBelowMag = 7;
        public int DepthClampToSeaOffset = 20;
        public double TickSeconds = 0.25;
        public bool ShardLoweringEnabled = true;
        public int ShardPatchCount = 8;
        public int ShardPatchRadiusMin = 5;
        public int ShardPatchRadiusMax = 16;
        public int ShardLowerMin = 1;
        public int ShardLowerMax = 8;
        public bool SoundsEnabled = true;
        public int InteriorScanHeight = 5;
        public int WarningDaysBefore = 3;

        // User-defined schedule (when EnableScheduledEvents is true)
        public List<UserScheduledEvent> UserSchedule = new List<UserScheduledEvent>();
    }

    public class UserScheduledEvent
    {
        public int Month = 5;
        public int Day = 15;
        public int Hour = 12;
        public List<int> Magnitudes = new List<int> { 5 };
    }

    [ProtoContract]
    public class ScheduledEvent
    {
        [ProtoMember(1)] public int Month;
        [ProtoMember(2)] public int Day;
        [ProtoMember(3)] public int Hour;
        [ProtoMember(4)] public List<int> Magnitudes = new List<int>();
    }

    [ProtoContract]
    public class EarthquakeSchedule
    {
        [ProtoMember(1)] public int Year;
        [ProtoMember(2)] public List<ScheduledEvent> Events = new List<ScheduledEvent>();
        [ProtoMember(3)] public long LastCheckedTick = 0;
    }

    public class EarthquakeModSystem : ModSystem
    {
        ICoreServerAPI sapi;
        EarthquakeConfig cfg;
        const string NetChannel = "earthquake.notify";
        IServerNetworkChannel serverChan;
        string lastTriggeredStamp;
        HashSet<string> warnedStamps = new HashSet<string>();
        EarthquakeSchedule schedule;
        const string ScheduleKey = "earthquake_schedule";

        void OnPlayerNowPlaying(IServerPlayer player)
        {
            var cal = sapi.World.Calendar;
            if (cal == null || schedule?.Events == null || schedule.Events.Count == 0) return;

            EnsureScheduleCurrent();
            SafeMonthDayHour(cal, out int year, out int month, out int day, out int hour);

            if (TryFindUpcomingEvent(month, day, hour, out var evt, out int daysUntil))
            {
                int magnitude = evt.Magnitudes?.FirstOrDefault() ?? 1;
                // Per-player send, does not rely on warnedStamps (so late joiners still get it)
                serverChan.SendPacket(new NotificationPacket
                {
                    Type = "warning",
                    Magnitude = magnitude,
                    DaysUntil = daysUntil
                }, player);
            }
        }
        bool TryFindUpcomingEvent(int month, int day, int hour, out ScheduledEvent evt, out int daysUntil)
        {
            evt = null;
            daysUntil = 0;

            int maxDays = GameMath.Clamp(cfg.WarningDaysBefore, 1, 4); 

            for (int da = 0; da <= maxDays; da++) // include 0 to allow “today”
            {
                int checkMonth = month;
                int checkDay = day + da;
                int daysInMonth = SafeDaysInMonth(sapi.World.Calendar, checkMonth);

                while (checkDay > daysInMonth)
                {
                    checkDay -= daysInMonth;
                    checkMonth++;
                    if (checkMonth > SafeMonthsPerYear(sapi.World.Calendar))
                    {
                        checkMonth = 1;
                    }
                    daysInMonth = SafeDaysInMonth(sapi.World.Calendar, checkMonth);
                }

                // Pick any event on that day
                var candidate = schedule.Events.FirstOrDefault(e => e.Month == checkMonth && e.Day == checkDay);
                if (candidate == null) continue;

                // If it's today, only warn when the event is still ahead in time (or at this hour)
                if (da == 0 && candidate.Hour < hour) continue;

                evt = candidate;
                daysUntil = da;
                return true;
            }

            return false;
        }


        // Hardcoded loot tables
        Dictionary<string, List<string>> loot = new Dictionary<string, List<string>>() {
            { "preshock_weak", new List<string> { "game:coal-brown", "game:coal-black", "game:clay-red", "game:clay-blue", "game:clay-fire", "game:clear-quartz", "game:ore-nativecopper-{rock}", "game:nugget-nativecopper", "game:amethyst", "game:flint", "game:gear-rusty", "game:gem-olivine_peridot-rough", "game:nugget-bismuthinite", "game:nugget-cassiterite", "game:nugget-galena", "game:nugget-hematite", "game:nugget-malachite", "game:ore-alum-{rock}", "game:ore-anthracite-{rock}", "game:ore-bituminouscoal-{rock}", "game:ore-borax-{rock}", "game:ore-olivine-{rock}", "game:stone-{rock}", "game:salt", "game:saltpeter", "game:rosequartz", "game:smokyquartz", "game:potash" } },
            { "preshock_medium", new List<string> { "game:clay-fire", "game:ore-copper-{rock}", "game:ore-halite-{rock}", "game:ore-lead-{rock}", "game:clear-quartz", "game:ore-tin-{rock}", "game:ore-silver-{rock}", "game:ore-gold-{rock}", "game:flint", "game:gear-rusty", "game:gem-emerald-rough", "game:gem-olivine_peridot-rough", "game:nugget-chromite", "game:nugget-nativecopper", "game:nugget-nativesilver", "game:nugget-nativegold", "game:ore-alum-{rock}", "game:ore-borax-{rock}", "game:salt", "game:saltpeter", "game:potash" } },
            { "preshock_strong", new List<string> { "game:clay-fire", "game:ore-copper-{rock}", "game:ore-halite-{rock}", "game:ore-lead-{rock}", "game:clear-quartz", "game:ore-tin-{rock}", "game:ore-silver-{rock}", "game:ore-gold-{rock}", "game:flint", "game:gear-rusty", "game:gem-emerald-rough", "game:gem-olivine_peridot-rough", "game:nugget-chromite", "game:nugget-nativecopper", "game:nugget-nativesilver", "game:nugget-nativegold", "game:ore-alum-{rock}", "game:ore-borax-{rock}", "game:salt", "game:saltpeter", "game:potash" } },
            { "quake_weak", new List<string> { "game:ore-copper-{rock}", "game:ore-halite-{rock}", "game:clay-fire", "game:ore-lead-{rock}", "game:clear-quartz", "game:flint", "game:gear-rusty", "game:gem-olivine_peridot-rough", "game:nugget-bismuthinite", "game:nugget-cassiterite", "game:nugget-galena", "game:nugget-hematite", "game:nugget-nativecopper", "game:ore-anthracite-{rock}", "game:ore-bituminouscoal-{rock}", "game:ore-borax-{rock}", "game:salt", "game:saltpeter", "game:rosequartz", "game:smokyquartz", "game:potash" } },
            { "quake_medium", new List<string> { "game:ore-tin-{rock}", "game:ore-silver-{rock}", "game:ore-gold-{rock}", "game:ore-borax", "game:ore-cinnabar", "game:ore-alum", "game:amethyst", "game:clay-fire", "game:flint", "game:gear-rusty", "game:gem-emerald-rough", "game:gem-olivine_peridot-rough", "game:nugget-nativesilver", "game:nugget-nativegold", "game:ore-borax-{rock}", "game:salt", "game:saltpeter", "game:powder-sulfur", "game:powder-sylvite", "game:powder-alum", "game:powder-borax", "game:powder-cinnabar", "game:powder-flint", "game:powder-lapislazuli", "game:potash" } },
            { "quake_strong", new List<string> { "game:ore-iron-{rock}", "game:ore-hematite-{rock}", "game:ore-uranium-{rock}", "game:clay-fire", "game:ore-sulfur-{rock}", "game:ore-fluorite-{rock}", "game:ore-corundum-{rock}", "game:ore-lapislazuli-{rock}", "game:gear-temporal", "game:gear-rusty", "game:gem-diamond-rough", "game:ore-phosphorite-{rock}", "game:powder-sulfur", "game:powder-sylvite", "game:powder-cinnabar", "game:powder-lapislazuli", "game:potash" } }
        };

        //I'M NOT REWRITING IT, YOU REWRITE IT (it works why 'fix' it)
        public override void StartServerSide(ICoreServerAPI api)
        {
            sapi = api;
            LoadConfig();
            serverChan = sapi.Network.RegisterChannel(NetChannel).RegisterMessageType<NotificationPacket>();
            sapi.Event.PlayerNowPlaying += OnPlayerNowPlaying;
            sapi.RegisterCommand("earthquake", "Earthquake tools and triggers", "/earthquake [showdates|gendates|test <magnitude>]",
                (IServerPlayer player, int gid, CmdArgs args) =>
                {
                    try
                    {
                        if (args.Length == 0)
                        {
                            player.SendMessage(GlobalConstants.GeneralChatGroup, "Usage: /earthquake [showdates|gendates|test <magnitude>]", EnumChatType.CommandError);
                            return;
                        }

                        string sub = args.PopWord().ToLowerInvariant();

                        if (sub == "showdates")
                        {
                            try
                            {
                                EnsureScheduleCurrent();
                                if (schedule?.Events == null || schedule.Events.Count == 0)
                                {
                                    player.SendMessage(GlobalConstants.GeneralChatGroup,
                                        "No scheduled earthquakes found for this year.", EnumChatType.Notification);
                                    return;
                                }

                                var dateStrings = schedule.Events
                                    .OrderBy(e => e.Month).ThenBy(e => e.Day).ThenBy(e => e.Hour)
                                    .Select(e => $"Month {e.Month}, Day {e.Day} @ {e.Hour:00}:00 - Magnitudes: {string.Join(", ", e.Magnitudes)}");

                                player.SendMessage(GlobalConstants.GeneralChatGroup,
                                    $"Scheduled earthquakes for year {schedule.Year}: {string.Join("; ", dateStrings)}",
                                    EnumChatType.Notification);
                            }
                            catch (Exception ex)
                            {
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Error: {ex.Message}", EnumChatType.CommandError);
                            }
                            return;
                        }

                        if (sub == "gendates")
                        {
                            try
                            {
                                GenerateScheduleForCurrentYear(true);
                                player.SendMessage(GlobalConstants.GeneralChatGroup,
                                    $"Earthquake schedule regenerated for year {schedule.Year}", EnumChatType.Notification);
                            }
                            catch (Exception ex)
                            {
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Error: {ex.Message}", EnumChatType.CommandError);
                            }
                            return;
                        }

                        if (sub == "test")
                        {
                            try
                            {
                                if (args.Length == 0 || !int.TryParse(args.PopWord(), out int mag))
                                {
                                    player.SendMessage(GlobalConstants.GeneralChatGroup, "Usage: /earthquake test <1-9>", EnumChatType.CommandError);
                                    return;
                                }
                                mag = GameMath.Clamp(mag, 1, 9);
                                var pos = player.Entity.ServerPos.AsBlockPos.Copy();
                                BroadcastPacket(new NotificationPacket { Type = "warning", Magnitude = mag, DaysUntil = 0 });
                                StartForeshockSequence(pos, mag, false);
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Triggered magnitude {mag} earthquake.", EnumChatType.Notification);
                            }
                            catch (Exception ex)
                            {
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Error: {ex.Message}", EnumChatType.CommandError);
                            }
                            return;
                        }

                        if (int.TryParse(sub, out int directMag))
                        {
                            try
                            {
                                directMag = GameMath.Clamp(directMag, 1, 9);
                                var pos = player.Entity.ServerPos.AsBlockPos.Copy();
                                BroadcastPacket(new NotificationPacket { Type = "warning", Magnitude = directMag, DaysUntil = 0 });
                                StartForeshockSequence(pos, directMag, false);
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Triggered magnitude {directMag} earthquake test.", EnumChatType.Notification);
                            }
                            catch (Exception ex)
                            {
                                player.SendMessage(GlobalConstants.GeneralChatGroup, $"Error: {ex.Message}", EnumChatType.CommandError);
                            }
                            return;
                        }

                        player.SendMessage(GlobalConstants.GeneralChatGroup, "Invalid command. Use: /earthquake showdates, /earthquake gendates, or /earthquake test <1-9>", EnumChatType.CommandError);
                    }
                    catch (Exception ex)
                    {
                        player.SendMessage(GlobalConstants.GeneralChatGroup, $"Command error: {ex.Message}", EnumChatType.CommandError);
                    }
                }, Privilege.controlserver);

            sapi.World.RegisterGameTickListener(CheckScheduleAndPlayers, 1000);
            EnsureScheduleCurrent();
        }

        public override void StartClientSide(ICoreClientAPI capi)
        {
            var chan = capi.Network.RegisterChannel(NetChannel).RegisterMessageType<NotificationPacket>();
            chan.SetMessageHandler<NotificationPacket>((packet) =>
            {
                if (packet.Type == "warning")
                {
                    capi.ShowChatMessage(Lang.Get("earthquake:warning-toast", packet.Magnitude, packet.DaysUntil));
                }
                else if (packet.Type == "foreshock")
                {
                    string foreshockKey = "earthquake:foreshock-weak";
                    if (packet.Magnitude >= 7) foreshockKey = "earthquake:foreshock-strong";
                    else if (packet.Magnitude >= 4) foreshockKey = "earthquake:foreshock-medium";

                    capi.ShowChatMessage(Lang.Get(foreshockKey));
                }
                else if (packet.Type == "complete")
                {
                    capi.ShowChatMessage(Lang.Get("earthquake:complete-toast", packet.Count, packet.Magnitude));
                }
            });
        }

        [ProtoContract]
        public class NotificationPacket
        {
            [ProtoMember(1)] public string Type;
            [ProtoMember(2)] public int Magnitude;
            [ProtoMember(3)] public int DaysUntil;
            [ProtoMember(4)] public int Count;
        }

        void LoadConfig()
        {
            cfg = sapi.LoadModConfig<EarthquakeConfig>("earthquake.config.json");
            if (cfg == null)
            {
                cfg = new EarthquakeConfig();
                cfg.EventsPerYear = GameMath.Clamp(cfg.EventsPerYear, 1, 3);
                cfg.QuakesPerEventMax = GameMath.Clamp(cfg.QuakesPerEventMax, 1, 4);
                cfg.WarningDaysBefore = GameMath.Clamp(cfg.WarningDaysBefore, 1, 4);
                sapi.StoreModConfig(cfg, "earthquake.config.json");
            }
            else
            {
                // Enforce limits
                cfg.EventsPerYear = GameMath.Clamp(cfg.EventsPerYear, 1, 3);
                cfg.QuakesPerEventMax = GameMath.Clamp(cfg.QuakesPerEventMax, 1, 4);
                cfg.WarningDaysBefore = GameMath.Clamp(cfg.WarningDaysBefore, 1, 4);
            }
        }

        void CheckScheduleAndPlayers(float dt)
        {
            var cal = sapi.World.Calendar;
            if (cal == null) return;

            EnsureScheduleCurrent();
            int year, month, day, hour;
            SafeMonthDayHour(cal, out year, out month, out day, out hour);

            // Check for warnings (X days before) - check every hour
            CheckForUpcomingWarnings(year, month, day, hour);

            // Check for earthquakes at exact time
            string stamp = $"{year:D4}-{month:D2}-{day:D2}-{hour:D2}";

            if (stamp == lastTriggeredStamp) return;

            var due = schedule?.Events?.FirstOrDefault(e => e.Month == month && e.Day == day && e.Hour == hour);
            if (due == null) return;

            lastTriggeredStamp = stamp;
            var players = sapi.World.AllOnlinePlayers?.OfType<IServerPlayer>().ToList();
            if (players == null || players.Count == 0) return;

            int quakeCount = GameMath.Clamp(1 + players.Count / 10, cfg.QuakesPerEventMin, cfg.QuakesPerEventMax);
            var rand = sapi.World.Rand;
            players = players.OrderBy(p => rand.Next()).ToList();
            int chosen = Math.Min(quakeCount, players.Count);

            BroadcastPacket(new NotificationPacket { Type = "foreshock", Magnitude = due.Magnitudes.FirstOrDefault() });

            for (int i = 0; i < chosen; i++)
            {
                var target = players[i];
                var tpos = target.Entity.ServerPos.AsBlockPos;
                int mag = due.Magnitudes[rand.Next(due.Magnitudes.Count)];
                StartForeshockSequence(tpos.Copy(), mag, true);
            }
        }

        void CheckForUpcomingWarnings(int year, int month, int day, int hour)
        {
            if (schedule?.Events == null) return;

            if (TryFindUpcomingEvent(month, day, hour, out var upcomingEvent, out int daysUntil))
            {
                // Unique stamp per event-day (prevents hourly spam but still allows join-time per-player sends)
                string warnStamp = $"{schedule.Year:D4}-{upcomingEvent.Month:D2}-{upcomingEvent.Day:D2}-D{daysUntil}";
                if (warnedStamps.Contains(warnStamp)) return;

                int magnitude = upcomingEvent.Magnitudes?.FirstOrDefault() ?? 1;
                BroadcastPacket(new NotificationPacket
                {
                    Type = "warning",
                    Magnitude = magnitude,
                    DaysUntil = daysUntil
                });
                warnedStamps.Add(warnStamp);
            }
        }


        void StartForeshockSequence(BlockPos center, int magnitude, bool broadcastOnFinish)
        {
            if (cfg.SoundsEnabled)
            {
                try
                {
                    float vol = GameMath.Clamp(magnitude / 9f, 0.05f, 1f);
                    sapi.World.PlaySoundAt(new AssetLocation("earthquake:sounds/quakes/earthquake-foreshock.ogg"),
                        center.X, center.Y, center.Z, null, false, 120f, vol);
                }
                catch { }
            }

            int steps = 12, step = 0;
            long id = 0L;
            id = sapi.World.RegisterGameTickListener((dt) =>
            {
                ForeshockHintCut(center, magnitude);
                var preshockTable = TableForPreshock(magnitude);
                int expandRadius = Math.Max(12, cfg.EventNearPlayerRadius / 10);
                SpawnFromTable(preshockTable, center, expandRadius, cfg.GoodiesPerEventMin / 2, magnitude);
                SpawnQuakeParticles(center, 28 + magnitude * 4, magnitude, 0.4f, true);

                step++;
                if (step >= steps)
                {
                    sapi.World.UnregisterGameTickListener(id);
                    TriggerEarthquake(center, magnitude, broadcastOnFinish);
                }
            }, 5000);
        }

        void ForeshockHintCut(BlockPos center, int magnitude)
        {
            var ba = sapi.World.BlockAccessor;
            var rand = sapi.World.Rand;
            int rays = GameMath.Clamp(2 + magnitude / 3, 2, 6);

            for (int i = 0; i < rays; i++)
            {
                double ang = rand.NextDouble() * GameMath.TWOPI;
                int len = rand.Next(12, 18);
                int depth = rand.Next(1, 3);

                for (int t = 0; t < len; t++)
                {
                    int xx = center.X + (int)(Math.Cos(ang) * t);
                    int zz = center.Z + (int)(Math.Sin(ang) * t);
                    for (int d = 0; d < depth; d++)
                    {
                        var p = new BlockPos(xx, center.Y - d, zz);
                        var b = ba.GetBlock(p);
                        if (IsCarvable(b)) ba.SetBlock(0, p);
                    }
                }
            }
        }

        void TriggerEarthquake(BlockPos pos, int mag, bool broadcastOnFinish)
        {
            var world = sapi.World;
            int radius = GameMath.Clamp(12 + mag * 8, 16, 120);
            int depth = GameMath.Clamp(6 + mag * 6, 8, 60);
            float duration = 2f + mag * 1.5f;
            int steps = Math.Max(1, (int)Math.Ceiling(duration / cfg.TickSeconds));

            var quakeTable = TableForQuake(mag);
            int baseCount = cfg.GoodiesPerEventMin;
            int quakeCount = baseCount + mag * 8;

            // Distribute loot across the area
            int lootPoints = Math.Max(3, radius / 20);
            for (int lp = 0; lp < lootPoints; lp++)
            {
                var rand = world.Rand;
                double ang = rand.NextDouble() * GameMath.TWOPI;
                int r = rand.Next(6, Math.Max(12, radius));
                int x = pos.X + (int)(Math.Cos(ang) * r);
                int z = pos.Z + (int)(Math.Sin(ang) * r);
                SpawnFromTable(quakeTable, new BlockPos(x, pos.Y, z), 16, quakeCount / lootPoints, mag);
            }

            if (cfg.SoundsEnabled) PlayExteriorSounds(pos, radius, mag);

            int faults = GameMath.Clamp(1 + mag / 2, 1, 6);
            List<FaultPlan> plans = new();
            var rand2 = world.Rand;

            for (int i = 0; i < faults; i++)
            {
                double angle = rand2.NextDouble() * GameMath.TWOPI;
                plans.Add(new FaultPlan()
                {
                    Angle = angle,
                    Radius = radius,
                    Depth = depth,
                    Center = pos.Copy(),
                    Width = GameMath.Clamp((mag >= 7 ? 2 : 1) + i % 2, 1, 4),
                    Jitter = 0.08 + mag * 0.01,
                    Magnitude = mag
                });
            }

            int step = 0;
            long listenerId = 0L;
            listenerId = sapi.World.RegisterGameTickListener((dt) =>
            {
                foreach (var plan in plans) CarveStep(plan, step, steps);

                SpawnQuakeParticles(pos, (int)(radius * 0.6), mag, 1f, false);

                if (step % 2 == 0)
                {
                    CleanupFloaters(pos, (int)(radius * 0.9));
                    GravelHaloBurst(pos, (int)(radius * (0.6 + mag * 0.04)));
                    if (cfg.ShardLoweringEnabled) LowerTerrainShards(pos, radius, mag);
                    CollapseTreesInRadius(pos, radius);
                }

                step++;
                if (step >= steps)
                {
                    sapi.World.UnregisterGameTickListener(listenerId);

                    if (broadcastOnFinish)
                    {
                        BroadcastPacket(new NotificationPacket { Type = "complete", Count = 1, Magnitude = mag });
                    }
                }
            }, (int)(cfg.TickSeconds * 1000));

            if (cfg.SoundsEnabled) PlayInteriorLoopsNearRooms(pos, radius, duration, mag >= 7);
        }

        void EnsureScheduleCurrent()
        {
            var cal = sapi.World.Calendar;
            int year = (int)(cal?.Year ?? 0);

            try
            {
                byte[] bytes = sapi.WorldManager.SaveGame.GetData(ScheduleKey);
                if (bytes != null)
                {
                    schedule = SerializerUtil.Deserialize<EarthquakeSchedule>(bytes);
                    if (schedule != null && schedule.Year == year && schedule.Events?.Count > 0)
                        return;
                }
            }
            catch { }

            GenerateScheduleForCurrentYear(false);
        }

        void GenerateScheduleForCurrentYear(bool forceRegenerate)
        {
            var cal = sapi.World.Calendar;
            int year = (int)(cal?.Year ?? 0);
            if (!forceRegenerate && schedule != null && schedule.Year == year && schedule.Events?.Count > 0)
                return;

            var evs = new List<ScheduledEvent>();

            // If EnableScheduledEvents is true AND user has defined custom schedule, use it
            if (cfg.EnableScheduledEvents && cfg.UserSchedule != null && cfg.UserSchedule.Count > 0)
            {
                foreach (var userEvent in cfg.UserSchedule)
                {
                    evs.Add(new ScheduledEvent
                    {
                        Month = userEvent.Month,
                        Day = userEvent.Day,
                        Hour = userEvent.Hour,
                        Magnitudes = new List<int>(userEvent.Magnitudes)
                    });
                }
            }
            else
            {
                // Random generation (from May to next May)
                int monthsPerYear = SafeMonthsPerYear(cal);
                var rng = sapi.World.Rand;
                var picked = new HashSet<string>();
                int eventCount = GameMath.Clamp(cfg.EventsPerYear, 1, 3);

                // Start from month 5 (May) - this is year start
                int attempts = 0;
                while (picked.Count < eventCount && attempts < eventCount * 10)
                {
                    int month = 5 + rng.Next(monthsPerYear);
                    if (month > monthsPerYear) month -= monthsPerYear;

                    int daysInMonth = SafeDaysInMonth(cal, month);
                    int day = 1 + rng.Next(daysInMonth);
                    string key = $"{month:D2}-{day:D2}";
                    picked.Add(key);
                    attempts++;
                }

                foreach (string dateKey in picked)
                {
                    var parts = dateKey.Split('-');
                    int month = int.Parse(parts[0]);
                    int day = int.Parse(parts[1]);
                    int hour = rng.Next(0, Math.Max(1, (int)Math.Ceiling(cal?.HoursPerDay ?? 24d)));

                    int magCount = rng.Next(1, 5);
                    var mags = new List<int>();
                    for (int m = 0; m < magCount; m++)
                    {
                        mags.Add(rng.Next(cfg.MagnitudeMin, cfg.MagnitudeMax + 1));
                    }

                    evs.Add(new ScheduledEvent { Month = month, Day = day, Hour = hour, Magnitudes = mags });
                }
            }

            schedule = new EarthquakeSchedule { Year = year, Events = evs, LastCheckedTick = sapi.World.ElapsedMilliseconds };

            // Clear warned stamps for new year
            warnedStamps.Clear();

            try
            {
                byte[] bytes = SerializerUtil.Serialize(schedule);
                sapi.WorldManager.SaveGame.StoreData(ScheduleKey, bytes);
            }
            catch { }
            lastTriggeredStamp = null;
        }

        void SafeMonthDayHour(IGameCalendar cal, out int y, out int m, out int d, out int h)
        {
            y = (int)(cal?.Year ?? 0);
            m = Math.Max(1, (int)Math.Round(Convert.ToDouble(cal?.Month ?? 1d)));
            h = (int)Math.Max(0, Math.Floor(Convert.ToDouble(cal?.HourOfDay ?? 0d)));
            d = 1;
            try
            {
                var pm = cal.GetType().GetProperty("DayOfMonth");
                if (pm != null) d = Math.Max(1, (int)Math.Round(Convert.ToDouble(pm.GetValue(cal))));
            }
            catch { d = 1; }
        }

        int SafeMonthsPerYear(IGameCalendar cal)
        {
            try
            {
                var prop = cal?.GetType().GetProperty("MonthsPerYear");
                if (prop != null) return Math.Max(1, (int)Math.Round(Convert.ToDouble(prop.GetValue(cal))));
            }
            catch { }
            return 12;
        }

        int SafeDaysInMonth(IGameCalendar cal, int month1Based)
        {
            try
            {
                var arrProp = cal?.GetType().GetProperty("DaysPerMonth");
                if (arrProp != null && arrProp.PropertyType == typeof(int[]))
                {
                    var arr = (int[])arrProp.GetValue(cal);
                    if (arr != null && arr.Length >= month1Based) return Math.Max(1, arr[month1Based - 1]);
                }
            }
            catch { }

            double dpm = 30d;
            try { dpm = Convert.ToDouble(cal?.DaysPerMonth); } catch { }
            return Math.Max(1, (int)Math.Floor(dpm + 0.00001));
        }

        void BroadcastPacket(NotificationPacket packet)
        {
            serverChan.BroadcastPacket(packet);
        }

        class FaultPlan
        {
            public double Angle;
            public int Radius;
            public int Depth;
            public int Width = 1;
            public double Jitter;
            public BlockPos Center;
            public int Magnitude;
        }

        string TableForPreshock(int magnitude)
        {
            if (magnitude <= 2) return "preshock_weak";
            if (magnitude <= 5) return "preshock_medium";
            return "preshock_strong";
        }

        string TableForQuake(int magnitude)
        {
            if (magnitude <= 2) return "quake_weak";
            if (magnitude <= 5) return "quake_medium";
            return "quake_strong";
        }

        string NearbyRock(BlockPos center)
        {
            var ba = sapi.World.BlockAccessor;
            for (int dy = 2; dy >= -8; dy--)
            {
                var bb = ba.GetBlock(center.AddCopy(0, dy, 0));
                var rock = bb?.Variant?["rock"];
                if (!string.IsNullOrEmpty(rock)) return rock;
            }
            return "granite";
        }

        IEnumerable<string> ExpandCodesWithRock(IEnumerable<string> codes, string rock)
        {
            foreach (var c in codes)
            {
                if (c != null && c.Contains("{rock}")) yield return c.Replace("{rock}", rock);
                else yield return c;
            }
        }

        void SpawnFromTable(string tableKey, BlockPos center, int radius, int totalCount, int magnitude)
        {
            if (string.IsNullOrEmpty(tableKey) || loot == null) return;
            if (!loot.TryGetValue(tableKey, out var list) || list == null || list.Count == 0) return;

            var rand = sapi.World.Rand;
            string rock = NearbyRock(center);
            var expanded = ExpandCodesWithRock(list, rock).ToList();

            for (int i = 0; i < totalCount; i++)
            {
                var code = expanded[rand.Next(expanded.Count)];
                var coll = ResolveByCodes(new string[] { code });
                if (coll == null) continue;

                var dropPos = RandomSurfaceAround(center, Math.Max(12, radius));
                var vel = new Vec3d((rand.NextDouble() - 0.5) * 0.2, 0.25 + rand.NextDouble() * 0.15, (rand.NextDouble() - 0.5) * 0.2);
                sapi.World.SpawnItemEntity(new ItemStack(coll, 1), dropPos, vel);
            }
        }

        CollectibleObject ResolveByCodes(IEnumerable<string> codes)
        {
            foreach (var code in codes)
            {
                var al = new AssetLocation(code);
                var item = sapi.World.GetItem(al);
                if (item != null) return item;
                var block = sapi.World.GetBlock(al);
                if (block != null) return block;
            }
            return null;
        }

        void CarveStep(FaultPlan plan, int step, int totalSteps)
        {
            var ba = sapi.World.BlockAccessor;
            var world = sapi.World;
            double t0 = (double)step / totalSteps;
            double t1 = (double)(step + 1) / totalSteps;

            int r0 = (int)Math.Round(plan.Radius * t0);
            int r1 = (int)Math.Round(plan.Radius * t1);
            if (r1 <= r0) r1 = r0 + 1;

            var rand = world.Rand;
            for (int r = r0; r < r1; r++)
            {
                double jitteredAngle = plan.Angle + (rand.NextDouble() - 0.5) * plan.Jitter;
                int x = plan.Center.X + (int)(Math.Cos(jitteredAngle) * r);
                int z = plan.Center.Z + (int)(Math.Sin(jitteredAngle) * r);

                int sea = sapi.World.SeaLevel;
                int topY = Math.Min(plan.Center.Y + 10, ba.MapSizeY - 3);
                int bottomLimit = (plan.Magnitude >= cfg.DepthClampBelowMag)
                    ? Math.Max(2, plan.Center.Y - plan.Depth)
                    : Math.Max(sea - cfg.DepthClampToSeaOffset, plan.Center.Y - plan.Depth);

                int bottomY = GameMath.Clamp(bottomLimit, 2, topY - 2);

                for (int dx = -plan.Width; dx <= plan.Width; dx++)
                    for (int dz = -plan.Width; dz <= plan.Width; dz++)
                    {
                        if (dx * dx + dz * dz > plan.Width * plan.Width) continue;
                        for (int y = topY; y >= bottomY; y--)
                        {
                            var pos = new BlockPos(x + dx, y, z + dz);
                            var b = ba.GetBlock(pos);
                            if (IsCarvable(b)) ba.SetBlock(0, pos);
                        }
                    }
            }
        }

        bool IsCarvable(Block b)
        {
            if (b == null) return false;
            var mat = b.BlockMaterial;
            return mat == EnumBlockMaterial.Stone ||
                   mat == EnumBlockMaterial.Gravel ||
                   mat == EnumBlockMaterial.Ore ||
                   mat == EnumBlockMaterial.Soil ||
                   mat == EnumBlockMaterial.Sand ||
                   mat == EnumBlockMaterial.Liquid;
        }

        Vec3d RandomSurfaceAround(BlockPos center, int radius)
        {
            var rand = sapi.World.Rand;
            double ang = rand.NextDouble() * GameMath.TWOPI;
            int r = rand.Next(6, Math.Max(12, radius - 2));
            int x = center.X + (int)(Math.Cos(ang) * r);
            int z = center.Z + (int)(Math.Sin(ang) * r);
            int y = FindSurfaceY(x, z, center.Y + 16);
            return new Vec3d(x + 0.5, y + 0.5, z + 0.5);
        }

        void CleanupFloaters(BlockPos center, int radius)
        {
            var ba = sapi.World.BlockAccessor;
            int minY = Math.Max(2, center.Y - 32);
            int maxY = Math.Min(ba.MapSizeY - 3, center.Y + 32);

            for (int x = center.X - radius; x <= center.X + radius; x += 1)
                for (int z = center.Z - radius; z <= center.Z + radius; z += 1)
                {
                    for (int y = maxY; y >= minY; y--)
                    {
                        var pos = new BlockPos(x, y, z);
                        var b = ba.GetBlock(pos);
                        if (b == null || b.BlockId == 0) continue;

                        var below = ba.GetBlock(pos.DownCopy());
                        bool unsupported = below == null || below.BlockId == 0;
                        if (!unsupported) continue;

                        // Remove floating plants, vegetation, and liquid
                        if (b.BlockMaterial == EnumBlockMaterial.Plant ||
                            b.BlockMaterial == EnumBlockMaterial.Leaves ||
                            b.BlockMaterial == EnumBlockMaterial.Liquid ||
                            b.Code.Path.Contains("grass") ||
                            b.Code.Path.Contains("plant") ||
                            b.Code.Path.Contains("flower") ||
                            b.Code.Path.Contains("bush") ||
                            b.Code.Path.Contains("reed") ||
                            b.Code.Path.Contains("wood") ||
                            b.Code.Path.Contains("log"))
                        {
                            ba.SetBlock(0, pos);
                        }
                        // Fill floating solid blocks with air to prevent floating islands
                        else if (b.BlockMaterial == EnumBlockMaterial.Stone ||
                                 b.BlockMaterial == EnumBlockMaterial.Soil ||
                                 b.BlockMaterial == EnumBlockMaterial.Sand ||
                                 b.BlockMaterial == EnumBlockMaterial.Gravel ||
                                 b.BlockMaterial == EnumBlockMaterial.Snow)
                        {
                            ba.SetBlock(0, pos);
                        }
                    }
                }
        }

        void GravelHaloBurst(BlockPos center, int radius)
        {
            var ba = sapi.World.BlockAccessor;
            var gravel = sapi.World.GetBlock(new AssetLocation("game:gravel-granite")) ?? sapi.World.GetBlock(new AssetLocation("game:gravel-bauxite"));
            if (gravel == null) return;

            var rand = sapi.World.Rand;
            int tries = Math.Max(20, radius);

            for (int i = 0; i < tries; i++)
            {
                double ang = rand.NextDouble() * GameMath.TWOPI;
                int r = rand.Next(Math.Max(6, radius / 3), radius);
                int x = center.X + (int)(Math.Cos(ang) * r);
                int z = center.Z + (int)(Math.Sin(ang) * r);
                int y = FindSurfaceY(x, z, center.Y + 12);

                var above = new BlockPos(x, y, z);
                if (ba.GetBlockId(above) == 0)
                {
                    ba.SetBlock(gravel.BlockId, above);
                    sapi.World.SpawnItemEntity(new ItemStack(gravel, 1),
                        new Vec3d(x + 0.5, y + 1.5, z + 0.5),
                        new Vec3d((rand.NextDouble() - 0.5) * 0.6, 0.6 + rand.NextDouble() * 0.3, (rand.NextDouble() - 0.5) * 0.6));
                }
            }
        }

        void LowerTerrainShards(BlockPos center, int radius, int mag)
        {
            var ba = sapi.World.BlockAccessor;
            var rand = sapi.World.Rand;
            int patches = Math.Max(2, cfg.ShardPatchCount * mag / 9);

            for (int p = 0; p < patches; p++)
            {
                int minR = Math.Min(cfg.ShardPatchRadiusMin, cfg.ShardPatchRadiusMax);
                int maxR = Math.Max(cfg.ShardPatchRadiusMin, cfg.ShardPatchRadiusMax);
                int pr = rand.Next(minR, maxR + 1);

                double ang = rand.NextDouble() * GameMath.TWOPI;
                int r = rand.Next(6, radius - pr - 2);
                int cx = center.X + (int)(Math.Cos(ang) * r);
                int cz = center.Z + (int)(Math.Sin(ang) * r);
                int drop = rand.Next(cfg.ShardLowerMin, cfg.ShardLowerMax + 1);

                int topY = Math.Min(center.Y + 8, ba.MapSizeY - 3);
                int bottomY = Math.Max(center.Y - 10, 2);

                for (int dx = -pr; dx <= pr; dx++)
                    for (int dz = -pr; dz <= pr; dz++)
                    {
                        if (dx * dx + dz * dz > pr * pr) continue;
                        for (int y = bottomY; y <= topY; y++)
                        {
                            var from = new BlockPos(cx + dx, y, cz + dz);
                            var to = new BlockPos(cx + dx, Math.Max(2, y - drop), cz + dz);
                            int id = ba.GetBlockId(from);
                            if (id == 0) continue;
                            ba.SetBlock(0, from);
                            ba.SetBlock(id, to);
                        }
                    }
            }
        }

        int FindSurfaceY(int x, int z, int fromY)
        {
            var ba = sapi.World.BlockAccessor;
            int y = Math.Min(fromY, ba.MapSizeY - 2);
            for (; y > 1; y--)
            {
                var b = ba.GetBlock(new BlockPos(x, y, z));
                var below = ba.GetBlock(new BlockPos(x, y - 1, z));
                if (b.BlockId == 0 && below.BlockId != 0) return y;
            }
            return Math.Max(2, fromY);
        }

        void SpawnQuakeParticles(BlockPos center, int radius, int magnitude, float intensityScale, bool isForeshock)
        {
            var rand = sapi.World.Rand;

            // Dust
            {
                int qty = (int)(40 * intensityScale) + magnitude * (isForeshock ? 1 : 3);
                for (int i = 0; i < qty; i++)
                {
                    double ang = rand.NextDouble() * GameMath.TWOPI;
                    int r = rand.Next(6, Math.Max(12, radius));
                    int x = center.X + (int)(Math.Cos(ang) * r);
                    int z = center.Z + (int)(Math.Sin(ang) * r);
                    int y = FindSurfaceY(x, z, center.Y + 8);

                    var dust = new SimpleParticleProperties()
                    {
                        MinQuantity = 1,
                        AddQuantity = 2,
                        Color = Rgba(80, 120, 110, 100),
                        MinPos = new Vec3d(x + 0.5, y + 0.2, z + 0.5),
                        AddPos = new Vec3d(0.2, 0.2, 0.2),
                        MinVelocity = new Vec3f(-0.05f, 0.05f, -0.05f),
                        AddVelocity = new Vec3f(0.05f, 0.15f, 0.05f),
                        LifeLength = 1.2f,
                        GravityEffect = 0.1f,
                        SizeEvolve = EvolvingNatFloat.create(EnumTransformFunction.LINEAR, -0.1f),
                        ParticleModel = EnumParticleModel.Quad
                    };
                    sapi.World.SpawnParticles(dust);
                }
            }

            // Rock chips
            {
                int qty = (int)(18 * intensityScale) + magnitude * (isForeshock ? 1 : 2);
                for (int i = 0; i < qty; i++)
                {
                    double ang = rand.NextDouble() * GameMath.TWOPI;
                    int r = rand.Next(4, Math.Max(10, radius / 2));
                    int x = center.X + (int)(Math.Cos(ang) * r);
                    int z = center.Z + (int)(Math.Sin(ang) * r);
                    int y = FindSurfaceY(x, z, center.Y + 6);

                    var chips = new SimpleParticleProperties()
                    {
                        MinQuantity = 1,
                        AddQuantity = 1,
                        Color = Rgba(255, 255, 255, 255),
                        MinPos = new Vec3d(x + 0.5, y + 0.6, z + 0.5),
                        MinVelocity = new Vec3f(-0.15f, 0.25f, -0.15f),
                        AddVelocity = new Vec3f(0.15f, 0.45f, 0.15f),
                        LifeLength = 0.9f,
                        GravityEffect = 0.2f,
                        ParticleModel = EnumParticleModel.Cube
                    };
                    sapi.World.SpawnParticles(chips);
                }
            }

            // Steam/geyser puffs
            {
                int geysers = (isForeshock ? 1 : 3) + magnitude / 3;
                for (int g = 0; g < geysers; g++)
                {
                    double ang = rand.NextDouble() * GameMath.TWOPI;
                    int r = rand.Next(6, Math.Max(10, radius));
                    int x = center.X + (int)(Math.Cos(ang) * r);
                    int z = center.Z + (int)(Math.Sin(ang) * r);
                    int y = FindSurfaceY(x, z, center.Y + 8);

                    var steam = new SimpleParticleProperties()
                    {
                        MinQuantity = 3,
                        AddQuantity = 6,
                        Color = Rgba(80, 230, 230, 230),
                        MinPos = new Vec3d(x + 0.5, y + 0.1, z + 0.5),
                        AddPos = new Vec3d(0.4, 0.4, 0.4),
                        MinVelocity = new Vec3f(0f, 0.05f, 0f),
                        AddVelocity = new Vec3f(0f, 0.15f, 0f),
                        LifeLength = 2.6f,
                        GravityEffect = -0.01f,
                        ParticleModel = EnumParticleModel.Quad
                    };
                    sapi.World.SpawnParticles(steam);
                }
            }
        }

        static int Rgba(int a, int r, int g, int b) => (a << 24) | (r << 16) | (g << 8) | b;

        void PlayExteriorSounds(BlockPos center, int radius, int mag)
        {
            try
            {
                int points = Math.Max(4, radius / 16);
                for (int i = 0; i < points; i++)
                {
                    double ang = (double)i / points * GameMath.TWOPI;
                    int x = center.X + (int)(Math.Cos(ang) * (radius / 2));
                    int z = center.Z + (int)(Math.Sin(ang) * (radius / 2));
                    sapi.World.PlaySoundAt(new AssetLocation("earthquake:sounds/quakes/earthquake-exterior.ogg"),
                        x, center.Y, z, null, false, 120f, 1f);
                }

                if (mag >= 7)
                {
                    for (int i = 0; i < Math.Max(1, points / 3); i++)
                    {
                        double ang = (double)i / Math.Max(1, points / 3) * GameMath.TWOPI;
                        int x = center.X + (int)(Math.Cos(ang) * (radius * 3 / 4));
                        int z = center.Z + (int)(Math.Sin(ang) * (radius * 3 / 4));
                        sapi.World.PlaySoundAt(new AssetLocation("earthquake:sounds/quakes/earthquake-large.ogg"),
                            x, center.Y, z, null, false, 120f, 0.8f);
                    }
                }
            }
            catch { }
        }

        void PlayInteriorLoopsNearRooms(BlockPos center, int radius, float duration, bool big)
        {
            try
            {
                var players = sapi.World.AllOnlinePlayers?.OfType<IServerPlayer>().ToList();
                if (players == null) return;
                foreach (var pl in players)
                {
                    var p = pl.Entity.ServerPos.AsBlockPos;
                    if (p.DistanceTo(center) > radius) continue;
                    if (!IsLikelyInterior(p)) continue;

                    sapi.World.PlaySoundAt(new AssetLocation("earthquake:sounds/quakes/earthquake-interior.ogg"),
                        pl, null, true, 120f, 0.8f);
                }
            }
            catch { }
        }

        bool IsLikelyInterior(BlockPos pos)
        {
            var ba = sapi.World.BlockAccessor;
            for (int i = 1; i <= Math.Max(1, cfg.InteriorScanHeight); i++)
            {
                var above = ba.GetBlock(new BlockPos(pos.X, pos.Y + i, pos.Z));
                if (above.BlockId == 0) return false;
            }
            return true;
        }

        void CollapseTreesInRadius(BlockPos center, int radius)
        {
            var ba = sapi.World.BlockAccessor;
            var rand = sapi.World.Rand;

            for (int x = center.X - radius; x <= center.X + radius; x++)
            {
                for (int z = center.Z - radius; z <= center.Z + radius; z++)
                {
                    // Find the top of the terrain
                    int y = center.Y + 20;
                    for (; y > center.Y - 20; y--)
                    {
                        var block = ba.GetBlock(new BlockPos(x, y, z));
                        if (block != null && block.BlockId != 0) break;
                    }

                    // Scan upward for tree logs/wood
                    for (int scanY = y; scanY < Math.Min(y + 30, ba.MapSizeY - 1); scanY++)
                    {
                        var pos = new BlockPos(x, scanY, z);
                        var block = ba.GetBlock(pos);

                        if (block == null || block.BlockId == 0) break;

                        if (block.Code.Path.Contains("log") || block.Code.Path.Contains("wood"))
                        {
                            // Found a tree, topple it
                            int height = 0;
                            for (int h = scanY; h < Math.Min(scanY + 40, ba.MapSizeY - 1); h++)
                            {
                                var checkBlock = ba.GetBlock(new BlockPos(x, h, z));
                                if (checkBlock == null || checkBlock.BlockId == 0) break;
                                if (!checkBlock.Code.Path.Contains("log") && !checkBlock.Code.Path.Contains("wood") &&
                                    checkBlock.BlockMaterial != EnumBlockMaterial.Leaves) break;
                                height++;
                            }

                            if (height > 0)
                            {
                                // Random direction to fall
                                double ang = rand.NextDouble() * GameMath.TWOPI;
                                int fallDir = rand.Next(0, 4);

                                // Remove logs and make them fall
                                for (int h = 0; h < height; h++)
                                {
                                    var logPos = new BlockPos(x, scanY + h, z);
                                    ba.SetBlock(0, logPos);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}