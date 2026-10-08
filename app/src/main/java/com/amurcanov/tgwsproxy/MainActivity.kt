package com.amurcanov.tgwsproxy

import android.Manifest
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.PowerManager
import android.provider.Settings
import android.util.Log
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatDelegate
import androidx.core.os.LocaleListCompat
import androidx.compose.animation.*
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.CubicBezierEasing
import androidx.compose.animation.core.tween
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.shape.GenericShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.ui.input.pointer.pointerInput

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.PowerSettingsNew
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material.icons.filled.Terminal
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.graphics.lerp
import androidx.compose.ui.graphics.luminance

import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.amurcanov.tgwsproxy.ui.AppUpdateDialog
import com.amurcanov.tgwsproxy.ui.ConnectionTab
import com.amurcanov.tgwsproxy.ui.FloatingToolbar
import com.amurcanov.tgwsproxy.ui.InfoTab
import com.amurcanov.tgwsproxy.ui.LogsTab
import com.amurcanov.tgwsproxy.ui.SettingsTab
import com.amurcanov.tgwsproxy.ui.TgWsProxyTheme
import com.amurcanov.tgwsproxy.ui.openUrlInBrowser
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.min
import kotlin.math.sin

import java.util.Locale

class MainActivity : ComponentActivity() {

    override fun attachBaseContext(newBase: Context) {
        val prefs = newBase.getSharedPreferences("lang_prefs", Context.MODE_PRIVATE)
        val lang = prefs.getString("app_language", "zh") ?: "zh"
        val locale = Locale(lang)
        Locale.setDefault(locale)
        val config = android.content.res.Configuration(newBase.resources.configuration)
        config.setLocale(locale)
        super.attachBaseContext(newBase.createConfigurationContext(config))
    }

    private val requestPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) {}

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            requestPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
        }
        
        androidx.core.view.WindowCompat.setDecorFitsSystemWindows(window, false)

        setContent {
            val context = LocalContext.current
            val settingsStore = remember { SettingsStore(context) }
            val themeMode by settingsStore.themeMode
                .collectAsStateWithLifecycle(initialValue = "system")
            val isDynamicColor by settingsStore.isDynamicColor
                .collectAsStateWithLifecycle(initialValue = true)
            val themePalette by settingsStore.themePalette
                .collectAsStateWithLifecycle(initialValue = "indigo")
            val scope = rememberCoroutineScope()

            LaunchedEffect(settingsStore) {
                settingsStore.migrateLegacyDefaults()
            }

            TgWsProxyTheme(themeMode = themeMode, dynamicColor = isDynamicColor, themePalette = themePalette) {
                androidx.compose.runtime.CompositionLocalProvider(
                    androidx.compose.ui.platform.LocalDensity provides androidx.compose.ui.unit.Density(
                        density = androidx.compose.ui.platform.LocalDensity.current.density,
                        fontScale = 1f
                    )
                ) {
                    Box(modifier = Modifier.fillMaxSize()) {
                        AppBackdrop(modifier = Modifier.matchParentSize())

                        Surface(
                            modifier = Modifier.fillMaxSize(),
                            color = Color.Transparent
                        ) {
                            Box {
                                MainContent(settingsStore)

                                FloatingToolbar(
                                    currentTheme = themeMode,
                                    onThemeChange = { mode ->
                                        scope.launch { settingsStore.saveThemeMode(mode) }
                                    },
                                    isDynamicColor = isDynamicColor,
                                    onDynamicColorChange = { dc ->
                                        scope.launch { settingsStore.saveDynamicColor(dc) }
                                    },
                                    currentPalette = themePalette,
                                    onPaletteChange = { pal ->
                                        scope.launch { settingsStore.saveThemePalette(pal) }
                                    }
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

private data class NavItem(
    val label: String,
    val iconRes: androidx.compose.ui.graphics.vector.ImageVector
)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainContent(settingsStore: SettingsStore) {
    var selectedTab by rememberSaveable { mutableIntStateOf(0) }
    var dragTargetIndex by remember { mutableIntStateOf(-1) }
    var dragProgress by remember { mutableFloatStateOf(0f) }
    val context = LocalContext.current
    val density = LocalDensity.current
    val scope = rememberCoroutineScope()
    val updatePostponeUntil by settingsStore.updatePostponeUntil.collectAsStateWithLifecycle(initialValue = 0L)
    val updatePostponeVersion by settingsStore.updatePostponeVersion.collectAsStateWithLifecycle(initialValue = "")
    val updateCheckIntervalHours by settingsStore.updateCheckIntervalHours.collectAsStateWithLifecycle(
        initialValue = UPDATE_CHECK_NEVER
    )
    val updateLastCheckAt by settingsStore.updateLastCheckAt.collectAsStateWithLifecycle(initialValue = 0L)
    var pendingRelease by remember { mutableStateOf<AppReleaseInfo?>(null) }
    val currentVersion = remember { "v${BuildConfig.VERSION_NAME.removePrefix("v")}" }
    val currentUpdatePostponeUntil by rememberUpdatedState(updatePostponeUntil)
    val currentUpdatePostponeVersion by rememberUpdatedState(updatePostponeVersion)
    val navItems = listOf(
        NavItem(stringResource(R.string.nav_proxy), Icons.Default.PowerSettingsNew),
        NavItem(stringResource(R.string.settings), Icons.Default.Settings),
        NavItem(stringResource(R.string.nav_logs), Icons.Default.Terminal),
        NavItem(stringResource(R.string.info), Icons.Default.Info)
    )
    val safeBottomInset = with(density) { WindowInsets.safeDrawing.getBottom(density).toDp() }
    val navOverlayReserve = safeBottomInset + 96.dp

    DisposableEffect(Unit) {
        LogManager.init(context.applicationContext)
        LogManager.startListening()
        onDispose { LogManager.stopListening() }
    }

    LaunchedEffect(updateCheckIntervalHours, updateLastCheckAt) {
        if (updateCheckIntervalHours == UPDATE_CHECK_NEVER) return@LaunchedEffect

        val intervalMillis = updateIntervalHoursToMillis(updateCheckIntervalHours)
            ?: updateIntervalHoursToMillis(DEFAULT_UPDATE_CHECK_INTERVAL_HOURS)
            ?: 12L * 60L * 60L * 1000L

        if (updateLastCheckAt > 0L) {
            val nextCheckAt = updateLastCheckAt + intervalMillis
            val now = System.currentTimeMillis()
            if (nextCheckAt > now) {
                delay(nextCheckAt - now)
            }
        }

        if (!isActive) return@LaunchedEffect

        val checkedAt = System.currentTimeMillis()
        val release = fetchLatestReleaseInfo(currentVersion)
        settingsStore.saveUpdateState(
            lastCheckAt = checkedAt,
            latestVersion = release?.versionTag ?: "",
            error = if (release == null) context.getString(R.string.update_check_failed_short) else ""
        )

        if (release == null) {
            Log.w("TgWsProxy", "[WARN] Update check: no release info, local=$currentVersion")
        } else {
            val hasUpdate = isNewerVersion(currentVersion, release.versionTag)
            val isPostponed =
                currentUpdatePostponeVersion == release.versionTag && checkedAt < currentUpdatePostponeUntil
            Log.i(
                "TgWsProxy",
                "Update check: local=$currentVersion remote=${release.versionTag} newer=$hasUpdate postponed=$isPostponed"
            )

            if (hasUpdate && !isPostponed) {
                settingsStore.saveUpdateDialogShown(release.versionTag, checkedAt)
                pendingRelease = release
            }
        }
    }

    Scaffold(
        contentWindowInsets = WindowInsets.safeDrawing.only(WindowInsetsSides.Horizontal + WindowInsetsSides.Top),
        containerColor = Color.Transparent,
    ) { padding ->
        Box(modifier = Modifier
            .fillMaxSize()
            .padding(padding)
            .consumeWindowInsets(padding)
            .pointerInput(selectedTab) {
                var totalDrag = 0f
                detectHorizontalDragGestures(
                    onDragStart = {
                        totalDrag = 0f
                        dragTargetIndex = -1
                        dragProgress = 0f
                    },
                    onDragCancel = {
                        dragTargetIndex = -1
                        dragProgress = 0f
                    },
                    onDragEnd = {
                        if (dragTargetIndex in navItems.indices && dragProgress >= 0.5f) {
                            selectedTab = dragTargetIndex
                        }
                        dragTargetIndex = -1
                        dragProgress = 0f
                    }
                ) { change, dragAmount ->
                    change.consume()
                    totalDrag += dragAmount
                    if (abs(totalDrag) < 12f) {
                        dragTargetIndex = -1
                        dragProgress = 0f
                        return@detectHorizontalDragGestures
                    }

                    val candidate = if (totalDrag < 0f) selectedTab + 1 else selectedTab - 1
                    if (candidate !in navItems.indices) {
                        dragTargetIndex = -1
                        dragProgress = 0f
                        return@detectHorizontalDragGestures
                    }

                    dragTargetIndex = candidate
                    dragProgress = (abs(totalDrag) / 180f).coerceIn(0f, 1f)
                }
            }
        ) {
            AnimatedContent(
                targetState = selectedTab,
                transitionSpec = {
                    fadeIn(tween(300)) togetherWith fadeOut(tween(225))
                },
                modifier = Modifier
                    .fillMaxSize()
                    .padding(bottom = navOverlayReserve),
                label = "tab_content"
            ) { page ->
                when (page) {
                    0 -> ConnectionTab(settingsStore)
                    1 -> SettingsTab(settingsStore)
                    2 -> LogsTab(settingsStore)
                    3 -> InfoTab(settingsStore)
                }
            }

            ProxyNavigationBar(
                navItems = navItems,
                selectedTab = selectedTab,
                dragTargetIndex = dragTargetIndex,
                dragProgress = dragProgress,
                onTabSelected = { index ->
                    selectedTab = index
                    dragTargetIndex = -1
                    dragProgress = 0f
                },
                modifier = Modifier.align(Alignment.BottomCenter)
            )
        }
    }

    pendingRelease?.let { release ->
        AppUpdateDialog(
            release = release,
            onPostpone = {
                pendingRelease = null
                Toast.makeText(context, context.getString(R.string.update_postponed_24h), Toast.LENGTH_SHORT).show()
                scope.launch {
                    val now = System.currentTimeMillis()
                    settingsStore.saveUpdatePostpone(
                        version = release.versionTag,
                        until = now + 24L * 60L * 60L * 1000L
                    )
                    settingsStore.saveUpdateDialogAction(
                        version = release.versionTag,
                        action = UPDATE_DIALOG_ACTION_POSTPONED,
                        actedAt = now
                    )
                }
            },
            onUpdate = {
                pendingRelease = null
                scope.launch {
                    settingsStore.saveUpdateDialogAction(
                        version = release.versionTag,
                        action = UPDATE_DIALOG_ACTION_UPDATE,
                        actedAt = System.currentTimeMillis()
                    )
                    openUrlInBrowser(context, release.releaseUrl)
                }
            }
        )
    }
}

@Composable
private fun ProxyNavigationBar(
    navItems: List<NavItem>,
    selectedTab: Int,
    dragTargetIndex: Int,
    dragProgress: Float,
    onTabSelected: (Int) -> Unit,
    modifier: Modifier = Modifier
) {
    val colors = MaterialTheme.colorScheme
    val isDark = colors.background.luminance() < 0.22f
    val selectedColor = colors.primary
    val unselectedColor = colors.onSurfaceVariant.copy(alpha = 0.55f)
    val shellColor = if (isDark) {
        colors.surface.copy(alpha = 0.78f)
    } else {
        lerp(colors.surface, colors.surfaceVariant, 0.48f).copy(alpha = 0.95f)
    }
    val shellBorder = if (isDark) {
        colors.outlineVariant.copy(alpha = 0.42f)
    } else {
        colors.outline.copy(alpha = 0.16f)
    }
    val indicatorColor = if (isDark) {
        colors.primaryContainer.copy(alpha = 0.84f)
    } else {
        lerp(colors.primaryContainer, colors.surface, 0.18f).copy(alpha = 0.97f)
    }
    val indicatorIndex = remember { Animatable(selectedTab.toFloat()) }
    val dragVisualIndex = indicatorIndex.value

    LaunchedEffect(selectedTab) {
        if (dragTargetIndex !in navItems.indices) {
            indicatorIndex.animateTo(
                targetValue = selectedTab.toFloat(),
                animationSpec = tween(
                    durationMillis = 720,
                    easing = CubicBezierEasing(0.2f, 0.9f, 0.24f, 1f)
                )
            )
        }
    }

    LaunchedEffect(selectedTab, dragTargetIndex, dragProgress) {
        if (dragTargetIndex in navItems.indices) {
            val target = selectedTab.toFloat() + (dragTargetIndex - selectedTab) * dragProgress
            indicatorIndex.snapTo(target)
        }
    }

    BoxWithConstraints(
        modifier = modifier
            .fillMaxWidth()
            .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Horizontal + WindowInsetsSides.Bottom))
            .padding(horizontal = 22.dp, vertical = 12.dp)
    ) {
        val trackPadding = 8.dp
        val itemWidth = (maxWidth - trackPadding * 2) / navItems.size
        val indicatorOffset = trackPadding + itemWidth * dragVisualIndex

        Surface(
            shape = RoundedCornerShape(28.dp),
            color = shellColor,
            border = BorderStroke(1.dp, shellBorder),
            tonalElevation = 0.dp,
            shadowElevation = if (isDark) 10.dp else 8.dp,
            modifier = Modifier.fillMaxWidth()
        ) {
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(72.dp)
            ) {
                Surface(
                    shape = RoundedCornerShape(22.dp),
                    color = indicatorColor,
                    modifier = Modifier
                        .offset(x = indicatorOffset)
                        .padding(vertical = 6.dp)
                        .width(itemWidth)
                        .fillMaxHeight()
                ) {}

                Row(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(horizontal = trackPadding, vertical = 6.dp)
                ) {
                    navItems.forEachIndexed { index, item ->
                        val emphasis = (1f - abs(index - dragVisualIndex)).coerceIn(0f, 1f)
                        val iconColor = lerp(unselectedColor, selectedColor, emphasis)

                        Column(
                            modifier = Modifier
                                .weight(1f)
                                .fillMaxHeight()
                                .clip(RoundedCornerShape(22.dp))
                                .clickable { onTabSelected(index) },
                            verticalArrangement = Arrangement.Center,
                            horizontalAlignment = Alignment.CenterHorizontally
                        ) {
                            Icon(
                                imageVector = item.iconRes,
                                contentDescription = item.label,
                                modifier = Modifier.size(22.dp),
                                tint = iconColor
                            )
                            Spacer(Modifier.height(4.dp))
                            Text(
                                text = item.label,
                                style = MaterialTheme.typography.labelSmall,
                                fontWeight = if (emphasis > 0.55f) FontWeight.SemiBold else FontWeight.Medium,
                                color = iconColor,
                                maxLines = 1
                            )
                        }
                    }
                }
            }
        }
    }
}

private fun android16OrbShape(points: Int, innerRatio: Float): Shape = GenericShape { size, _ ->
    val centerX = size.width / 2f
    val centerY = size.height / 2f
    val outerRadius = min(size.width, size.height) / 2f
    val innerRadius = outerRadius * innerRatio

    for (i in 0 until points * 2) {
        val angle = (-PI / 2.0) + (i * PI / points)
        val radius = if (i % 2 == 0) outerRadius else innerRadius
        val x = centerX + (radius * cos(angle)).toFloat()
        val y = centerY + (radius * sin(angle)).toFloat()
        if (i == 0) moveTo(x, y) else lineTo(x, y)
    }
    close()
}

private val Android16OrbLarge: Shape = android16OrbShape(points = 18, innerRatio = 0.90f)
private val Android16OrbMedium: Shape = android16OrbShape(points = 20, innerRatio = 0.92f)
private val Android16OrbSmall: Shape = android16OrbShape(points = 16, innerRatio = 0.88f)

@Composable
private fun AppBackdrop(modifier: Modifier = Modifier) {
    val colors = MaterialTheme.colorScheme
    val isDark = colors.background.luminance() < 0.22f
    val baseBrush = remember(colors.background, colors.surface, colors.surfaceVariant) {
        Brush.verticalGradient(
            colors = if (isDark) {
                listOf(
                    lerp(colors.background, colors.surface, 0.42f),
                    colors.background,
                    lerp(colors.surfaceVariant, colors.background, 0.35f)
                )
            } else {
                listOf(
                    lerp(colors.background, colors.surface, 0.78f),
                    colors.background,
                    lerp(colors.surfaceVariant, colors.background, 0.30f)
                )
            }
        )
    }
    val topGlow = colors.primary.copy(alpha = if (isDark) 0.16f else 0.09f)
    val leftGlow = if (isDark) {
        colors.tertiary.copy(alpha = 0.11f)
    } else {
        lerp(colors.tertiary, colors.secondaryContainer, 0.74f).copy(alpha = 0.24f)
    }
    val bottomGlow = if (isDark) {
        colors.primary.copy(alpha = 0.10f)
    } else {
        lerp(colors.secondary, colors.primaryContainer, 0.70f).copy(alpha = 0.22f)
    }
    val lightOrbOutline = colors.outlineVariant.copy(alpha = 0.26f)
    val topOrbGlow = if (isDark) {
        topGlow
    } else {
        lerp(colors.primary, colors.primaryContainer, 0.72f).copy(alpha = 0.32f)
    }

    Box(
        modifier = modifier
            .fillMaxSize()
            .background(baseBrush)
    ) {
        Box(
            modifier = Modifier
                .align(Alignment.TopStart)
                .offset(x = (-86).dp, y = (-126).dp)
                .size(258.dp)
                .clip(Android16OrbLarge)
                .background(topOrbGlow)
                .then(
                    if (isDark) Modifier else Modifier.border(1.dp, lightOrbOutline, Android16OrbLarge)
                )
        )
        Box(
            modifier = Modifier
                .align(Alignment.CenterStart)
                .offset(x = (-44).dp, y = 28.dp)
                .size(146.dp)
                .clip(Android16OrbSmall)
                .background(leftGlow)
                .then(
                    if (isDark) Modifier else Modifier.border(1.dp, lightOrbOutline.copy(alpha = 0.22f), Android16OrbSmall)
                )
        )
        Box(
            modifier = Modifier
                .align(Alignment.BottomEnd)
                .offset(x = 62.dp, y = (-208).dp)
                .size(198.dp)
                .clip(Android16OrbMedium)
                .background(bottomGlow)
                .then(
                    if (isDark) Modifier else Modifier.border(1.dp, lightOrbOutline.copy(alpha = 0.20f), Android16OrbMedium)
                )
        )
    }
}

/**
 * Optimized LogManager: uses a Channel + batching approach to avoid
 * creating a new list on every single log line — reduces GC pressure
 * and eliminates UI jank caused by high-frequency log updates.
 *
 * Key optimizations:
 * - Channel-based buffering: log lines are queued, not applied immediately
 * - Batch processing: up to 20 lines applied per tick (every 150ms)
 * - Array-backed list with cap of 50: avoids growing/shrinking allocations
 * - Duplicate merging: last-entry count increment done in-place conceptually
 */
object LogManager {
    val logs = MutableStateFlow<List<LogEntry>>(emptyList())
    private var job: Job? = null
    private var logcatProcess: Process? = null
    private val nextKey = AtomicLong(0)
    private var appContext: Context? = null
    private val fileLock = Any()

    // Buffered channel — absorbs bursts of log lines without blocking the reader
    private val logChannel = Channel<LogEntry>(capacity = BUFFERED)

    private const val LOG_FILE_NAME = "tgwsproxy.log"
    private const val LOG_FILE_MAX_BYTES = 512 * 1024
    private const val LOG_FILE_KEEP_BYTES = 256 * 1024

    /** 绑定进程上下文，用于持久化日志文件。在 UI 组合时调用一次即可。 */
    fun init(context: Context) {
        if (appContext == null) {
            appContext = context.applicationContext
        }
    }

    /**
     * 直接写入一条日志（不依赖 logcat），同时持久化到文件。
     * 全链路埋点统一走这里，保证日志面板一定能看到。
     */
    fun add(message: String, priority: Int = Log.INFO, isError: Boolean = false, isEssential: Boolean = false) {
        val entry = LogEntry(
            key = "log_${nextKey.getAndIncrement()}",
            message = message,
            count = 1,
            isError = isError,
            priority = priority,
            isEssential = isEssential
        )
        logChannel.trySend(entry)
        appendFile(entry)
    }

    fun startListening() {
        if (job?.isActive == true) return
        job = CoroutineScope(Dispatchers.IO).launch {
            // Start logcat reader coroutine
            val readerJob = launch(Dispatchers.IO) {
                try {
                    val pid = android.os.Process.myPid()
                    val process = ProcessBuilder("logcat", "-v", "tag", "--pid", pid.toString())
                        .redirectErrorStream(true)
                        .start()

                    logcatProcess = process

                    process.inputStream.bufferedReader().use { reader ->
                        while (isActive) {
                            val line = try { reader.readLine() } catch (e: Exception) { null } ?: break
                            val entry = parseLine(line) ?: continue
                            logChannel.trySend(entry)
                        }
                    }
                } catch (e: Exception) {
                    add("日志通道(logcat)不可用: ${e.message}", Log.WARN, isEssential = true)
                } finally {
                    logcatProcess?.destroy()
                    logcatProcess = null
                }
            }

            // Batch consumer: collects queued entries and applies in batches
            launch {
                val pendingBatch = mutableListOf<LogEntry>()
                while (isActive) {
                    // Drain the channel (non-blocking)
                    var received = logChannel.tryReceive()
                    while (received.isSuccess) {
                        pendingBatch.add(received.getOrThrow())
                        if (pendingBatch.size >= 20) break // cap batch size
                        received = logChannel.tryReceive()
                    }

                    if (pendingBatch.isNotEmpty()) {
                        // 持久化 + 一次性更新 UI 状态
                        pendingBatch.forEach { appendFile(it) }
                        logs.value = applyBatch(logs.value, pendingBatch)
                        pendingBatch.clear()
                    }

                    // Throttle updates — 150ms between UI refreshes
                    delay(150)
                }
            }

            readerJob.join()
        }
    }

    /**
     * Efficiently applies a batch of new entries to the current log list.
     * Merges consecutive duplicates and caps at 50 entries.
     */
    private fun applyBatch(current: List<LogEntry>, batch: List<LogEntry>): List<LogEntry> {
        val result = ArrayDeque(current)
        for (entry in batch) {
            var merged = false
            val searchDepth = minOf(result.size, 10)
            for (i in result.indices.reversed().take(searchDepth)) {
                if (result[i].message == entry.message) {
                    val existing = result.removeAt(i)
                    result.addLast(existing.copy(count = existing.count + 1))
                    merged = true
                    break
                }
            }
            if (!merged) {
                result.addLast(entry)
            }
        }
        while (result.size > 50) {
            result.removeFirst()
        }
        return result.toList()
    }

    fun stopListening() {
        job?.cancel()
        job = null
        logcatProcess?.destroy()
        logcatProcess = null
    }

    fun clearLogs() {
        logs.value = emptyList()
        appContext?.let { ctx ->
            synchronized(fileLock) {
                try {
                    File(ctx.filesDir, LOG_FILE_NAME).delete()
                } catch (_: Exception) {
                }
            }
        }
    }

    /** 追加一行到持久化文件（带时间戳与级别），超过 512KB 时保留尾部 256KB。 */
    private fun appendFile(entry: LogEntry) {
        val ctx = appContext ?: return
        synchronized(fileLock) {
            try {
                val file = File(ctx.filesDir, LOG_FILE_NAME)
                if (file.length() > LOG_FILE_MAX_BYTES) {
                    trimFile(file)
                }
                val ts = java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", java.util.Locale.US)
                    .format(java.util.Date())
                val level = when (entry.priority) {
                    6 -> "E"
                    5 -> "W"
                    3 -> "D"
                    else -> "I"
                }
                file.appendText("[$ts][$level] ${entry.message}\n")
            } catch (_: Exception) {
            }
        }
    }

    private fun trimFile(file: File) {
        try {
            val raf = java.io.RandomAccessFile(file, "rw")
            val len = raf.length()
            if (len > LOG_FILE_KEEP_BYTES) {
                raf.seek(len - LOG_FILE_KEEP_BYTES)
                val buf = ByteArray(LOG_FILE_KEEP_BYTES)
                raf.readFully(buf)
                raf.setLength(0)
                raf.seek(0)
                raf.write(buf)
            }
            raf.close()
        } catch (_: Exception) {
        }
    }

    private fun parseLine(raw: String): LogEntry? {
        // logcat -v tag 输出格式 "Tag: msg"；同时兼容 "L/Tag: msg" 形式
        val tagLevel = Regex("^([VDIWEF])/([^:]+): (.*)$").find(raw)
        val plain = if (tagLevel == null) Regex("^([^:]+): (.*)$").find(raw) else null

        val level = tagLevel?.groupValues?.get(1) ?: "I"
        val tag = tagLevel?.groupValues?.get(2) ?: plain?.groupValues?.get(1) ?: return null
        var msg = tagLevel?.groupValues?.get(3) ?: plain?.groupValues?.get(2) ?: ""

        var priority = when (level) {
            "E" -> 6
            "W" -> 5
            "D", "V" -> 3
            else -> 4
        }
        var isError = level == "E"

        // 消息内容里的显式级别前缀优先
        when {
            msg.contains("[ERROR]") -> {
                msg = msg.substringAfter("[ERROR]").trim()
                priority = 6
                isError = true
            }
            msg.contains("[WARN]") -> {
                msg = msg.substringAfter("[WARN]").trim()
                priority = 5
            }
            msg.contains("[DEBUG]") -> {
                msg = msg.substringAfter("[DEBUG]").trim()
                priority = 3
            }
            tag == "System.err" && priority == 4 -> priority = 5 // Java/JNA 异常输出
        }

        // 去除 ↑3.3KB ↓1.1KB 0.3秒 这类动态指标，让日志行可以折叠
        if (msg.contains("↑")) {
            msg = msg.substringBefore("↑").trim()
        }
        if (msg.contains("↓")) {
            msg = msg.substringBefore("↓").trim()
        }

        // Remove emojis and stickers
        val emojiRegex = Regex("[\\x{1F300}-\\x{1F5FF}\\x{1F900}-\\x{1F9FF}\\x{1F600}-\\x{1F64F}\\x{1F680}-\\x{1F6FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{1F1E6}-\\x{1F1FF}\\x{1F191}-\\x{1F251}\\x{1F004}\\x{1F0CF}\\x{1F170}-\\x{1F171}\\x{1F17E}-\\x{1F17F}\\x{1F18E}\\x{3030}\\x{2B50}\\x{2B55}\\x{2934}-\\x{2935}\\x{2B05}-\\x{2B07}\\x{2B1B}-\\x{2B1C}\\x{3297}\\x{3299}\\x{303D}\\x{00A9}\\x{00AE}\\x{2122}\\x{23F3}\\x{24C2}\\x{23E9}-\\x{23EF}\\x{25B6}\\x{23F8}-\\x{23FA}⚠✅❌⚡🔥🔄🔗]")
        msg = msg.replace(emojiRegex, "").trim()

        if (msg.isEmpty()) return null

        val isEssential = listOf(
            "pool", "key:", "started", "address:", "error", "failed", "blocked",
            "StartProxy", "StopProxy", "端口", "port", "超时", "异常", "启动", "停止", "bind", "listen", "返回",
            "池", "密钥:", "已启动", "地址:", "错误", "失败", "已封锁", "不可用", "占用"
        ).any { marker -> msg.contains(marker, ignoreCase = true) }

        return LogEntry(
            key = "log_${nextKey.getAndIncrement()}",
            message = msg,
            count = 1,
            isError = isError,
            priority = priority,
            isEssential = isEssential
        )
    }
}
