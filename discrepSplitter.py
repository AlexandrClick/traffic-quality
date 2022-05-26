def calc_ctp(node_sums):
    return node_sums['pixels'] / node_sums['clicks']


def calc_rss(node_sums):
    ctp = calc_ctp(node_sums)
    return node_sums['pixels2'] - 2 * ctp * node_sums['clicks_x_pixels'] + ctp ** 2 * node_sums['clicks2']


def update_sums(node_sums, clck, pix, is_add=True):
    is_add_coefficient = 1 if is_add else -1
    node_sums['clicks'] += is_add_coefficient * clck
    node_sums['clicks2'] += is_add_coefficient * clck ** 2
    node_sums['pixels'] += is_add_coefficient * pix
    node_sums['pixels2'] += is_add_coefficient * pix ** 2
    node_sums['clicks_x_pixels'] += is_add_coefficient * clck * pix
    return node_sums


def unite_sums(*nodes_sums):
    united_sums = {}
    for node_sums in nodes_sums:
        for key in node_sums:
            united_sums[key] = united_sums.get(key, 0) + node_sums[key]

    return united_sums


class DiscrepSplitter:
    def __init__(self,
                 min_clicks_leaf=100,
                 min_pixels_leaf=100,
                 min_slices_leaf=3):
        self.min_clicks_leaf = min_clicks_leaf
        self.min_pixels_leaf = min_pixels_leaf
        self.min_slices_leaf = min_slices_leaf
        self.thresholds = []
        self.leaf_sums = {}
        self.leaf_splits = {}

    def get_thresholds(self):
        return sorted([1 - thr for thr in self.thresholds])

    def _find_new_split(self):
        max_improvement = 0
        best_borders, best_split = None, 0
        best_left_sums, best_right_sums = None, None
        for left_border, right_border in self.leaf_sums:
            if self.leaf_splits[(left_border, right_border)]['max_improvement'] is None:
                self.leaf_splits[(left_border, right_border)]['max_improvement'] = 0
                left_sums = {
                    'clicks': 0,
                    'pixels': 0,
                    'clicks2': 0,
                    'pixels2': 0,
                    'clicks_x_pixels': 0
                }
                right_sums = self.leaf_sums[(left_border, right_border)].copy()

                best_error = calc_rss(right_sums)

                for i in range(left_border, right_border - 1):
                    left_sums = update_sums(left_sums, self.clicks[i], self.pixels[i], is_add=True)
                    right_sums = update_sums(right_sums, self.clicks[i], self.pixels[i], is_add=False)

                    if min(i - left_border + 1, right_border - i - 1) < self.min_slices_leaf:
                        continue
                    if min(left_sums['pixels'], right_sums['pixels']) < self.min_pixels_leaf:
                        continue
                    if min(left_sums['clicks'], right_sums['clicks']) < self.min_clicks_leaf:
                        continue
                    if self.ctps[i] == self.ctps[i + 1]:
                        continue

                    error_left = calc_rss(left_sums)
                    error_right = calc_rss(right_sums)
                    improvement = best_error - error_left - error_right

                    if improvement > self.leaf_splits[(left_border, right_border)]['max_improvement']:
                        self.leaf_splits[(left_border, right_border)]['max_improvement'] = improvement
                        self.leaf_splits[(left_border, right_border)]['best_left_sums'] = left_sums.copy()
                        self.leaf_splits[(left_border, right_border)]['best_right_sums'] = right_sums.copy()
                        self.leaf_splits[(left_border, right_border)]['best_split'] = i + 1

            if self.leaf_splits[(left_border, right_border)]['max_improvement'] > max_improvement:
                max_improvement = self.leaf_splits[(left_border, right_border)]['max_improvement']
                best_borders = (left_border, right_border)
        best_split = self.leaf_splits[best_borders]['best_split']
        self.leaf_sums[(best_borders[0], best_split)] = self.leaf_splits[best_borders]['best_left_sums']
        self.leaf_sums[(best_split, best_borders[1])] = self.leaf_splits[best_borders]['best_right_sums']
        self.leaf_splits[(best_borders[0], best_split)] = {'max_improvement': None}
        self.leaf_splits[(best_split, best_borders[1])] = {'max_improvement': None}
        self.thresholds += [(self.ctps[best_split] + self.ctps[best_split + 1]) / 2]
        del self.leaf_sums[best_borders]
        del self.leaf_splits[best_borders]

    def fit(self, clicks, pixels, max_leaves=5):
        ctps = [p / c for p, c in zip(pixels, clicks)]
        self.ctps, self.clicks, self.pixels = list(zip(*sorted(zip(ctps, clicks, pixels), key=lambda x: x[0])))

        root_sums = {
            'clicks': sum(self.clicks),
            'pixels': sum(self.pixels),
            'clicks2': sum([clck ** 2 for clck in self.clicks]),
            'pixels2': sum([pix ** 2 for pix in self.pixels]),
            'clicks_x_pixels': sum([self.clicks[i] * self.pixels[i] for i in range(len(self.ctps))])
        }

        self.leaf_sums[(0, len(ctps))] = root_sums.copy()
        self.leaf_splits[(0, len(ctps))] = {'max_improvement': None}

        for split in range(max_leaves - 1):
            self._find_new_split()

        return self
