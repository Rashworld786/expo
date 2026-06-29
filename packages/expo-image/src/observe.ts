import type { ExpoAppMetricsModuleType } from 'expo-app-metrics';
import { requireOptionalNativeModule } from 'expo-modules-core';
import type { ObserveIntegrationsConfig, ObserveModule } from 'expo-observe';
import { Dimensions, PixelRatio } from 'react-native';

import type { ImageNativeModule } from './Image.types';

/**
 * Configuration for the `expo-observe` integration, set through
 * `Observe.configure({ integrations: { 'expo-image': ... } })`. Passing `true` enables it with
 * defaults; the object form tunes the behavior.
 *
 * The `declare module 'expo-observe'` augmentation that registers the `'expo-image'` key lives in
 * `Image.types.ts` (always in the package's public type graph via `export *`, so it is picked up
 * whenever expo-image is imported). `Image.types.ts` `import type`s this from here. It is exported
 * from this module but not from the package entry, so it is not part of the public API.
 */
export type ExpoImageIntegrationConfig = {
  /**
   * An image is reported as oversized when its decoded pixel area exceeds the screen's area (scaled
   * by the device pixel ratio) by more than this factor. For example, `2` flags an image decoded at
   * more than twice the pixels a full-screen image would need.
   *
   * @default 2
   */
  ratio?: number;
};

const DEFAULT_RATIO = 2;

// Guards `initObserveIntegrationIfNeeded` so repeated calls wire up the integration only once.
let initialized = false;

// Exported for testing purposes only.
export type IntegrationState = {
  enabled: boolean;
  threshold: number;
  // URLs already reported under the current configuration. Only oversized images are added.
  reported: Set<string>;
  // Subscription to the native `onImageLoaded` event, held only while the integration is enabled.
  subscription: { remove: () => void } | null;
  // Native modules resolved when the integration initializes; either may be absent.
  appMetrics: ExpoAppMetricsModuleType | null;
  imageModule: ImageNativeModule | null;
};

type Integrations = ObserveIntegrationsConfig & {
  'expo-image'?: ExpoImageIntegrationConfig;
};

// Exported for testing purposes only.
export type LoadedImage = {
  url: string;
  width: number;
  height: number;
  screenWidth: number;
  screenHeight: number;
  pixelRatio: number;
};

// Exported for testing purposes only.
export function reportIfOversized(state: IntegrationState, image: LoadedImage): void {
  if (!state.enabled || !state.appMetrics) {
    return;
  }
  const { url, width, height, screenWidth, screenHeight, pixelRatio } = image;
  if (!url || !(width > 0) || !(height > 0)) {
    return;
  }
  if (state.reported.has(url)) {
    return;
  }
  const budget = screenWidth * screenHeight * pixelRatio;
  if (!(budget > 0) || width * height <= budget * state.threshold) {
    return;
  }
  state.reported.add(url);
  try {
    state.appMetrics.logEvent('expo-image.oversized', {
      severity: 'warn',
      body: `Image loaded at ${width}×${height}px is far larger than this device's screen (${screenWidth}×${screenHeight}pt @${pixelRatio}x). Constrain it with the maxWidth/maxHeight load options.`,
      attributes: {
        url,
        imageWidth: width,
        imageHeight: height,
        screenWidth,
        screenHeight,
        pixelRatio,
      },
    });
  } catch {
    console.warn('[expo-image] Failed to logEvent for oversized image.');
    // Reporting is best-effort; a logging failure must not disrupt image loading.
  }
}

// Exported for testing purposes only.
export function handleImageLoaded(
  state: IntegrationState,
  image: { url: string; width: number; height: number },
  report = reportIfOversized
): void {
  const screen = Dimensions.get('screen');
  report(state, {
    ...image,
    screenWidth: screen.width,
    screenHeight: screen.height,
    pixelRatio: PixelRatio.get(),
  });
}

// Exported for testing purposes only.
export function activate(
  state: IntegrationState,
  integrations: Integrations,
  handle = handleImageLoaded
): void {
  const config = integrations['expo-image'];
  state.enabled = !!config;
  state.threshold =
    typeof config === 'object' && config !== null ? (config.ratio ?? DEFAULT_RATIO) : DEFAULT_RATIO;
  // A new configure may change the threshold (or enable the integration), so start a fresh dedup
  // set: images already reported under the previous settings become eligible to report again.
  state.reported = new Set<string>();
  if (state.enabled && !state.subscription && state.imageModule) {
    state.subscription = state.imageModule.addListener('onImageLoaded', (image) =>
      handle(state, image)
    );
  } else if (!state.enabled && state.subscription) {
    state.subscription.remove();
    state.subscription = null;
  }
}

// Exported for testing purposes only.
export function initObserveIntegrationIfNeededImpl(
  activate: (state: IntegrationState, integrations: Integrations) => void
): void {
  const observe = requireOptionalNativeModule<ObserveModule>('ExpoObserve');
  if (!observe) {
    return;
  }
  const state: IntegrationState = {
    enabled: false,
    threshold: DEFAULT_RATIO,
    reported: new Set<string>(),
    subscription: null,
    appMetrics: requireOptionalNativeModule<ExpoAppMetricsModuleType>('ExpoAppMetrics'),
    imageModule: requireOptionalNativeModule<ImageNativeModule>('ExpoImage'),
  };
  activate(state, observe.getIntegrations());
  observe.addListener('onConfigure', ({ integrations }) => activate(state, integrations));
}

/**
 * Wires the expo-observe oversized-image integration. Idempotent.
 */
export function initObserveIntegrationIfNeeded(): void {
  if (initialized) {
    return;
  }
  initialized = true;
  initObserveIntegrationIfNeededImpl(activate);
}
